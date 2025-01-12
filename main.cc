#include <unistd.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <cerrno>
#include <iostream>
#include <string>
#include <boost/program_options.hpp>

using namespace std;

struct device_info {
    union ibv_gid gid;
    uint32_t send_qp_num, write_qp_num;
    struct ibv_mr write_mr;
};

struct rdma_context_per_peer {
    string remote_ip_str;
    struct ibv_cq *send_cq = nullptr;
    struct ibv_cq *write_cq = nullptr;
    struct ibv_qp *send_qp = nullptr;
    struct ibv_qp *write_qp = nullptr;
    struct ibv_qp_init_attr qp_init_attr{};
    struct device_info local{}, remote{};
    char data_send[100], data_write[100];
    struct ibv_mr *send_mr = nullptr;
    struct ibv_mr *write_mr = nullptr;
    struct ibv_qp_attr qp_attr{};
};

struct rdma_context {
    bool server = false;
    int num_devices = 0;
    uint32_t gidIndex = 0;
    string ip_str, dev_str;

    struct ibv_device **dev_list = nullptr;
    struct ibv_context *context = nullptr;
    struct ibv_pd *pd = nullptr;
    struct ibv_port_attr port_attr{};
    struct ibv_gid_entry gidEntries[255];
    struct ibv_sge sg_send{}, sg_write{}, sg_recv{};
    struct ibv_send_wr wr_send{}, *bad_wr_send = nullptr, wr_write{}, *bad_wr_write = nullptr;
    struct ibv_recv_wr wr_recv{}, *bad_wr_recv = nullptr;
    struct ibv_mr remote_write_mr{};
    struct ibv_wc wc{};
    vector<rdma_context_per_peer> peers;
};

void parse_program_options(int argc, char *argv[], rdma_context &ctx) {
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
            ("help", "show possible options")
            ("dev", boost::program_options::value<string>(), "rdma device to use")
            ("src_ip", boost::program_options::value<string>(), "source ip")
            ("dst_ip", boost::program_options::value<vector<string> >()->multitoken(), "destination ips")
            ("server", "run as server");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    if (vm.count("help")) {
        cout << desc << endl;
        exit(0);
    }

    if (vm.count("dev"))
        ctx.dev_str = vm["dev"].as<string>();
    else
        cerr << "the --dev argument is required" << endl;

    if (vm.count("src_ip"))
        ctx.ip_str = vm["src_ip"].as<string>();
    else
        cerr << "the --src_ip argument is required" << endl;

    if (vm.count("dst_ip"))
        for (int i = 0; i < vm["dst_ip"].as<vector<string> >().size(); i++) {
            rdma_context_per_peer peer;
            peer.remote_ip_str = vm["dst_ip"].as<vector<string> >()[i];
            ctx.peers.push_back(peer);
        }
    else
        cerr << "the --dst_ip argument is required" << endl;

    if (vm.count("server"))
        ctx.server = true;
}

void get_device(rdma_context &ctx) {
    // populate dev_list using ibv_get_device_list - use num_devices as argument
    ctx.dev_list = ibv_get_device_list(&ctx.num_devices);
    if (!ctx.dev_list) {
        cerr << "ibv_get_device_list failed: " << strerror(errno) << endl;
        exit(1);
    }

    for (int i = 0; i < ctx.num_devices; i++) {
        // get the device name, using ibv_get_device_name
        auto dev = ibv_get_device_name(ctx.dev_list[i]);
        if (!dev) {
            cerr << "ibv_get_device_name failed: " << strerror(errno) << endl;
            ibv_free_device_list(ctx.dev_list);
        }

        // compare it to the device provided in the program arguments (dev_str)
        // and open the device; store the device context in "context"
        if (strcmp(dev, ctx.dev_str.c_str()) == 0) {
            ctx.context = ibv_open_device(ctx.dev_list[i]);
            break;
        }
    }
}

void allocate_pd(rdma_context &ctx) {
    // allocate a PD (protection domain), using ibv_alloc_pd
    ctx.pd = ibv_alloc_pd(ctx.context);
    if (!ctx.pd) {
        cerr << "ibv_alloc_pd failed: " << strerror(errno) << endl;
        ibv_close_device(ctx.context);
        exit(1);
    }
}

void create_cqs(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        // create a CQ (completion queue) for the send operations, using ibv_create_cq
        ctx.peers[i].send_cq = ibv_create_cq(ctx.context, 0x10, nullptr, nullptr, 0);
        if (!ctx.peers[i].send_cq) {
            cerr << "ibv_create_cq - send - failed: " << strerror(errno) << endl;
            ibv_dealloc_pd(ctx.pd);
            exit(1);
        }

        // create a CQ for the write operations, using ibv_create_cq
        ctx.peers[i].write_cq = ibv_create_cq(ctx.context, 0x10, nullptr, nullptr, 0);
        if (!ctx.peers[i].write_cq) {
            cerr << "ibv_create_cq - recv - failed: " << strerror(errno) << endl;
            ibv_destroy_cq(ctx.peers[i].send_cq);
            exit(1);
        }
    }
}

void create_qps(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        memset(&ctx.peers[i].qp_init_attr, 0, sizeof(ctx.peers[i].qp_init_attr));

        ctx.peers[i].qp_init_attr.recv_cq = ctx.peers[i].send_cq;
        ctx.peers[i].qp_init_attr.send_cq = ctx.peers[i].send_cq;
        ctx.peers[i].qp_init_attr.qp_type = IBV_QPT_RC;
        ctx.peers[i].qp_init_attr.sq_sig_all = 1;
        ctx.peers[i].qp_init_attr.cap.max_send_wr = 5;
        ctx.peers[i].qp_init_attr.cap.max_recv_wr = 5;
        ctx.peers[i].qp_init_attr.cap.max_send_sge = 1;
        ctx.peers[i].qp_init_attr.cap.max_recv_sge = 1;

        // create a QP (queue pair) for the send operations, using ibv_create_qp
        ctx.peers[i].send_qp = ibv_create_qp(ctx.pd, &ctx.peers[i].qp_init_attr);
        if (!ctx.peers[i].send_qp) {
            cerr << "ibv_create_qp failed: " << strerror(errno) << endl;
            ibv_destroy_cq(ctx.peers[i].send_cq);
            ibv_destroy_cq(ctx.peers[i].write_cq);
            exit(1);
        }

        ctx.peers[i].qp_init_attr.recv_cq = ctx.peers[i].write_cq;
        ctx.peers[i].qp_init_attr.send_cq = ctx.peers[i].write_cq;

        // create a QP for the write operations, using ibv_create_qp
        ctx.peers[i].write_qp = ibv_create_qp(ctx.pd, &ctx.peers[i].qp_init_attr);
        if (!ctx.peers[i].write_qp) {
            cerr << "ibv_create_qp failed: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.peers[i].send_qp);
            ibv_destroy_cq(ctx.peers[i].send_cq);
            ibv_destroy_cq(ctx.peers[i].write_cq);
            exit(1);
        }
    }
}

void modify_qps_init(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        memset(&ctx.peers[i].qp_attr, 0, sizeof(ctx.peers[i].qp_attr));

        ctx.peers[i].qp_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        ctx.peers[i].qp_attr.port_num = 1;
        ctx.peers[i].qp_attr.pkey_index = 0;
        ctx.peers[i].qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                                               IBV_ACCESS_REMOTE_WRITE |
                                               IBV_ACCESS_REMOTE_READ;

        // move both QPs in the INIT state, using ibv_modify_qp
        int ret = ibv_modify_qp(ctx.peers[i].send_qp, &ctx.peers[i].qp_attr,
                                IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (ret != 0) {
            cerr << "ibv_modify_qp - INIT - failed: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.peers[i].send_qp);
            exit(1);
        }

        ret = ibv_modify_qp(ctx.peers[i].write_qp, &ctx.peers[i].qp_attr,
                            IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (ret != 0) {
            cerr << "ibv_modify_qp - INIT - failed: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.peers[i].write_qp);
            exit(1);
        }
    }
}

void find_gid(rdma_context &ctx) {
    // use ibv_query_port to get information about port number 1
    if (ibv_query_port(ctx.context, 1, &ctx.port_attr)) {
        cerr << "ibv_query_port failed: " << strerror(errno) << endl;
        exit(1);
    }
    // fill gidEntries with the GID table entries of the port, using ibv_query_gid_table
    ibv_query_gid_table(ctx.context, ctx.gidEntries, ctx.port_attr.gid_tbl_len, 0);
    // if (ibv_query_gid_table(ctx.context, ctx.gidEntries, ctx.port_attr.gid_tbl_len, 0)) {
    // cerr << "ibv_query_gid_table failed: " << strerror(errno) << endl;
    // exit(1);
    // }
    for (auto &entry: ctx.gidEntries) {
        // we want only RoCEv2
        if (entry.gid_type != IBV_GID_TYPE_ROCE_V2)
            continue;

        in6_addr addr;
        memcpy(&addr, &entry.gid.global, sizeof(addr));

        char interface_id[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &addr, interface_id, INET6_ADDRSTRLEN);

        uint32_t ip;
        inet_pton(AF_INET, interface_id + strlen("::ffff:"), &ip);

        if (strncmp(ctx.ip_str.c_str(), interface_id + strlen("::ffff:"), INET_ADDRSTRLEN) == 0) {
            ctx.gidIndex = entry.gid_index;
            for (int i = 0; i < ctx.peers.size(); i++) {
                memcpy(&ctx.peers[i].local.gid, &entry.gid, sizeof(ctx.peers[i].local.gid));
            }
            break;
        }
    }

    // GID index 0 should never be used
    if (ctx.gidIndex == 0) {
        cerr << "Given IP not found in GID table" << endl;
        exit(1);
    }
}

void register_memory(rdma_context &ctx) {
    auto flags = IBV_ACCESS_LOCAL_WRITE |
                 IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_READ;

    for (int i = 0; i < ctx.peers.size(); i++) {
        // register the "data_send" and "data_write" buffers for RDMA operations, using ibv_reg_mr;
        // store the resulting mrs in send_mr and write_mr
        ctx.peers[i].send_mr = ibv_reg_mr(ctx.pd, ctx.peers[i].data_send, sizeof(ctx.peers[i].data_send), flags);
        if (!ctx.peers[i].send_mr) {
            cerr << "ibv_reg_mr failed: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.peers[i].write_qp);
            exit(1);
        }

        ctx.peers[i].write_mr = ibv_reg_mr(ctx.pd, ctx.peers[i].data_write, sizeof(ctx.peers[i].data_write), flags);
        if (!ctx.peers[i].write_mr) {
            cerr << "ibv_reg_mr failed: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].send_mr);
            exit(1);
        }

        memcpy(&ctx.peers[i].local.write_mr, ctx.peers[i].write_mr, sizeof(ctx.peers[i].local.write_mr));
        ctx.peers[i].local.send_qp_num = ctx.peers[i].send_qp->qp_num;
        ctx.peers[i].local.write_qp_num = ctx.peers[i].write_qp->qp_num;
    }
}

void modify_qps_rtr(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        memset(&ctx.peers[i].qp_attr, 0, sizeof(ctx.peers[i].qp_attr));

        ctx.peers[i].qp_attr.path_mtu = ctx.port_attr.active_mtu;
        ctx.peers[i].qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        ctx.peers[i].qp_attr.rq_psn = 0;
        ctx.peers[i].qp_attr.max_dest_rd_atomic = 1;
        ctx.peers[i].qp_attr.min_rnr_timer = 0;
        ctx.peers[i].qp_attr.ah_attr.is_global = 1;
        ctx.peers[i].qp_attr.ah_attr.sl = 0;
        ctx.peers[i].qp_attr.ah_attr.src_path_bits = 0;
        ctx.peers[i].qp_attr.ah_attr.port_num = 1;

        memcpy(&ctx.peers[i].qp_attr.ah_attr.grh.dgid, &ctx.peers[i].remote.gid, sizeof(ctx.peers[i].remote.gid));

        ctx.peers[i].qp_attr.ah_attr.grh.flow_label = 0;
        ctx.peers[i].qp_attr.ah_attr.grh.hop_limit = 5;
        ctx.peers[i].qp_attr.ah_attr.grh.sgid_index = ctx.gidIndex;
        ctx.peers[i].qp_attr.ah_attr.grh.traffic_class = 0;

        ctx.peers[i].qp_attr.ah_attr.dlid = 1;
        ctx.peers[i].qp_attr.dest_qp_num = ctx.peers[i].remote.send_qp_num;

        // move the send QP into the RTR state, using ibv_modify_qp
        int ret = ibv_modify_qp(ctx.peers[i].send_qp, &ctx.peers[i].qp_attr, IBV_QP_STATE | IBV_QP_AV |
                                                                             IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                                                                             IBV_QP_RQ_PSN |
                                                                             IBV_QP_MAX_DEST_RD_ATOMIC |
                                                                             IBV_QP_MIN_RNR_TIMER);

        if (ret != 0) {
            cerr << "ibv_modify_qp - RTR - failed: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].write_mr);
            exit(1);
        }

        ctx.peers[i].qp_attr.dest_qp_num = ctx.peers[i].remote.write_qp_num;

        // move the write QP into the RTR state, using ibv_modify_qp
        ret = ibv_modify_qp(ctx.peers[i].write_qp, &ctx.peers[i].qp_attr, IBV_QP_STATE | IBV_QP_AV |
                                                                          IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                                                                          IBV_QP_RQ_PSN |
                                                                          IBV_QP_MAX_DEST_RD_ATOMIC |
                                                                          IBV_QP_MIN_RNR_TIMER);

        if (ret != 0) {
            cerr << "ibv_modify_qp - RTR - failed: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].write_mr);
            exit(1);
        }
    }
}

void modify_qps_rts(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        ctx.peers[i].qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        ctx.peers[i].qp_attr.timeout = 0;
        ctx.peers[i].qp_attr.retry_cnt = 7;
        ctx.peers[i].qp_attr.rnr_retry = 7;
        ctx.peers[i].qp_attr.sq_psn = 0;
        ctx.peers[i].qp_attr.max_rd_atomic = 0;

        // move the send and write QPs into the RTS state, using ibv_modify_qp
        int ret = ibv_modify_qp(ctx.peers[i].send_qp, &ctx.peers[i].qp_attr,
                                IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        if (ret != 0) {
            cerr << "ibv_modify_qp - RTS - failed: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].write_mr);
            exit(1);
        }
        if (ctx.peers[i].qp_attr.qp_state != IBV_QPS_RTS) {
            cerr << "QP not in RTS state for peer " << i << endl;
            exit(1);
        } else {
            cout << "QP is in RTS state for peer " << i << endl;
        }

        ret = ibv_modify_qp(ctx.peers[i].write_qp, &ctx.peers[i].qp_attr,
                            IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        if (ret != 0) {
            cerr << "ibv_modify_qp - RTS - failed: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].write_mr);
            exit(1);
        }
    }
}

int send_data(const struct device_info &data, string ip) {
    int sockfd;
    struct sockaddr_in servaddr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
        return 1;

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(ip.c_str());
    servaddr.sin_port = htons(8080);

    if (connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) != 0)
        return 1;

    write(sockfd, &data, sizeof(data));
    close(sockfd);

    return 0;
}

int receive_data(struct device_info &data) {
    int sockfd, connfd, len;
    struct sockaddr_in servaddr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
        return 1;

    memset(&servaddr, 0, sizeof(servaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(8080);

    if ((bind(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr))) != 0)
        return 1;

    if ((listen(sockfd, 5)) != 0)
        return 1;

    connfd = accept(sockfd, NULL, NULL);
    if (connfd < 0)
        return 1;

    read(connfd, &data, sizeof(data));

    close(sockfd);

    return 0;
}

int receive_data_for_all_peers(vector<device_info> &peers) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        return 1;
    }

    int reuse = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        return 1;
    }

    sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(8080);

    if (bind(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0) {
        return 1;
    }
    if (listen(sockfd, peers.size()) < 0) {
        return 1;
    }

    for (int i = 0; i < peers.size(); i++) {
        int connfd = accept(sockfd, nullptr, nullptr);
        if (connfd < 0) {
            return 1;
        }
        read(connfd, &peers[i], sizeof(peers[i]));
        close(connfd);
    }
    close(sockfd);
    return 0;
}

int exchange_data_info(rdma_context &ctx) {
    if (ctx.server) {
        // Accept connections from all peers
        vector<device_info> remotes(ctx.peers.size());
        if (receive_data_for_all_peers(remotes) != 0) {
            return 1;
        }

        // Copy what was received into each peer
        for (int i = 0; i < ctx.peers.size(); i++) {
            ctx.peers[i].remote = remotes[i];
            if (send_data(ctx.peers[i].local, ctx.peers[i].remote_ip_str) != 0) {
                return 1;
            }
        }
    } else {
        for (auto &p: ctx.peers) {
            if (send_data(p.local, p.remote_ip_str) != 0) {
                return 1;
            }
            if (receive_data(p.remote) != 0) {
                return 1;
            }
        }
    }
    return 0;
}


void server_flow(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        memset(ctx.peers[i].data_write, 0, sizeof(ctx.peers[i].data_write));
        memset(ctx.peers[i].data_send, 0, sizeof(ctx.peers[i].data_send));
        memcpy(ctx.peers[i].data_write, "Hello, but with write", 21);

        memset(&ctx.sg_write, 0, sizeof(ctx.sg_write));
        ctx.sg_write.addr = (uintptr_t) ctx.peers[i].write_mr->addr;
        ctx.sg_write.length = sizeof(ctx.peers[i].data_write);
        ctx.sg_write.lkey = ctx.peers[i].write_mr->lkey;

        memset(&ctx.wr_write, 0, sizeof(ctx.wr_write));
        ctx.wr_write.wr_id = 0;
        ctx.wr_write.sg_list = &ctx.sg_write;
        ctx.wr_write.num_sge = 1;
        ctx.wr_write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        ctx.wr_write.send_flags = IBV_SEND_SIGNALED;
        ctx.wr_write.imm_data = htonl(0x1234);
        ctx.wr_write.wr.rdma.remote_addr = (uintptr_t) ctx.peers[i].remote.write_mr.addr;
        ctx.wr_write.wr.rdma.rkey = ctx.peers[i].remote.write_mr.rkey;

        int ret = ibv_post_send(ctx.peers[i].write_qp, &ctx.wr_write, &ctx.bad_wr_write);
        if (ret != 0) {
            exit(1);
        }

        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.peers[i].send_mr->addr;
        ctx.sg_recv.length = sizeof(ctx.peers[i].data_send);
        ctx.sg_recv.lkey = ctx.peers[i].send_mr->lkey;

        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.wr_id = 0;
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;
        ret = ibv_post_recv(ctx.peers[i].send_qp, &ctx.wr_recv, &ctx.bad_wr_recv);
        if (ret != 0) {
            exit(1);
        }

        do {
            ret = ibv_poll_cq(ctx.peers[i].send_cq, 1, &ctx.wc);
        } while (ret == 0);

        if (ctx.wc.status != IBV_WC_SUCCESS) {
            exit(1);
        }

        cout << ctx.peers[i].data_send << endl;
    }
}

void client_flow(rdma_context &ctx) {
    for (int i = 0; i < ctx.peers.size(); i++) {
        memset(ctx.peers[i].data_write, 0, sizeof(ctx.peers[i].data_write));
        memset(ctx.peers[i].data_send, 0, sizeof(ctx.peers[i].data_send));
        memcpy(ctx.peers[i].data_send, "Hello", 5);

        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.peers[i].write_mr->addr;
        ctx.sg_recv.length = sizeof(ctx.peers[i].data_write);
        ctx.sg_recv.lkey = ctx.peers[i].write_mr->lkey;

        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.wr_id = 0;
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;

        int ret = ibv_post_recv(ctx.peers[i].write_qp, &ctx.wr_recv, &ctx.bad_wr_recv);
        if (ret != 0) {
            exit(1);
        }

        do {
            ret = ibv_poll_cq(ctx.peers[i].write_cq, 1, &ctx.wc);
        } while (ret == 0);
        if (ctx.wc.status != IBV_WC_SUCCESS) {
            exit(1);
        }

        cout << ctx.peers[i].data_write << endl;

        memset(&ctx.sg_send, 0, sizeof(ctx.sg_send));
        ctx.sg_send.addr = (uintptr_t) ctx.peers[i].send_mr->addr;
        ctx.sg_send.length = sizeof(ctx.peers[i].data_send);
        ctx.sg_send.lkey = ctx.peers[i].send_mr->lkey;

        memset(&ctx.wr_send, 0, sizeof(ctx.wr_send));
        ctx.wr_send.wr_id = 0;
        ctx.wr_send.sg_list = &ctx.sg_send;
        ctx.wr_send.num_sge = 1;
        ctx.wr_send.opcode = IBV_WR_SEND;
        ctx.wr_send.send_flags = IBV_SEND_SIGNALED;

        ret = ibv_post_send(ctx.peers[i].send_qp, &ctx.wr_send, &ctx.bad_wr_send);
        if (ret != 0) {
            exit(1);
        }
    }
}


int main(int argc, char *argv[]) {
    rdma_context ctx;
    parse_program_options(argc, argv, ctx);
    get_device(ctx);
    allocate_pd(ctx);
    create_cqs(ctx);
    create_qps(ctx);
    modify_qps_init(ctx);
    find_gid(ctx);
    register_memory(ctx);
    if (exchange_data_info(ctx) != 0) exit(1);

    modify_qps_rtr(ctx);
    modify_qps_rts(ctx);

    if (ctx.server)
        server_flow(ctx);
    else
        client_flow(ctx);

    return 0;
}
