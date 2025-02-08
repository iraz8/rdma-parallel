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
    struct ibv_mr send_mr;
};

struct rdma_context_per_peer {
    string remote_ip_str;
    struct ibv_cq *send_cq = nullptr;
    struct ibv_cq *write_cq = nullptr;
    struct ibv_qp *send_qp = nullptr;
    struct ibv_qp *write_qp = nullptr;
    struct ibv_qp_init_attr qp_init_attr{};
    struct device_info local{}, remote{};
    char data_send[512], data_write[512];
    struct ibv_mr *send_mr = nullptr;
    struct ibv_mr *write_mr = nullptr;
    struct ibv_qp_attr qp_attr{};
};

struct rdma_context {
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
    string message;
};

void parse_program_options(int argc, char *argv[], rdma_context &ctx) {
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
            ("help", "show possible options")
            ("dev", boost::program_options::value<string>(), "rdma device to use")
            ("src_ip", boost::program_options::value<string>(), "source ip")
            ("dst_ip", boost::program_options::value<vector<string> >()->multitoken(), "destination ips")
            ("message", boost::program_options::value<string>(), "Message to send");

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

    if (vm.count("message")) {
        ctx.message = vm["message"].as<string>();
    }
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
    auto flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

    for (int i = 0; i < ctx.peers.size(); i++) {
        ctx.peers[i].send_mr = ibv_reg_mr(ctx.pd, ctx.peers[i].data_send,
                                          sizeof(ctx.peers[i].data_send), flags);
        if (!ctx.peers[i].send_mr) {
            cerr << "ibv_reg_mr failed for send_mr: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.peers[i].write_qp);
            exit(1);
        }

        ctx.peers[i].write_mr = ibv_reg_mr(ctx.pd, ctx.peers[i].data_write,
                                           sizeof(ctx.peers[i].data_write), flags);
        if (!ctx.peers[i].write_mr) {
            cerr << "ibv_reg_mr failed for write_mr: " << strerror(errno) << endl;
            ibv_dereg_mr(ctx.peers[i].send_mr);
            exit(1);
        }

        // Copy both into ctx.peers[i].local
        memcpy(&ctx.peers[i].local.write_mr, ctx.peers[i].write_mr, sizeof(ctx.peers[i].local.write_mr));
        memcpy(&ctx.peers[i].local.send_mr, ctx.peers[i].send_mr, sizeof(ctx.peers[i].local.send_mr));

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

static in_addr_t parse_ip(const std::string &ip_str) {
    in_addr addr;
    inet_aton(ip_str.c_str(), &addr);
    return addr.s_addr;
}

int all_to_all_exchange_data(rdma_context &ctx) {
    cout << "[DEBUG] Entering all-to-all exchange" << endl;

    // Common listening socket for 'accept' side
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) return 1;
    int reuse = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(8080);
    if (bind(listenfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0) return 1;
    if (listen(listenfd, ctx.peers.size() * 2) < 0) return 1;

    in_addr_t local_ip = parse_ip(ctx.ip_str);

    for (int i = 0; i < ctx.peers.size(); i++) {
        in_addr_t remote_ip = parse_ip(ctx.peers[i].remote_ip_str);
        bool iAmSmaller = (ntohl(local_ip) < ntohl(remote_ip));

        if (iAmSmaller) {
            bool connected = false;
            for (int attempts = 0; attempts < 10; attempts++) {
                int cfd = socket(AF_INET, SOCK_STREAM, 0);
                if (cfd < 0) {
                    cout << "[DEBUG] Socket creation failed for peer " << i << endl;
                    continue;
                }
                sockaddr_in peeraddr;
                memset(&peeraddr, 0, sizeof(peeraddr));
                peeraddr.sin_family = AF_INET;
                peeraddr.sin_addr.s_addr = remote_ip;
                peeraddr.sin_port = htons(8080);

                if (connect(cfd, (struct sockaddr *) &peeraddr, sizeof(peeraddr)) == 0) {
                    cout << "[DEBUG] Connected to " << ctx.peers[i].remote_ip_str << endl;
                    write(cfd, &ctx.peers[i].local, sizeof(ctx.peers[i].local));
                    read(cfd, &ctx.peers[i].remote, sizeof(ctx.peers[i].remote));
                    close(cfd);
                    connected = true;
                    break;
                } else {
                    cout << "[DEBUG] Connect failed with " << ctx.peers[i].remote_ip_str
                            << " (attempt " << attempts + 1 << "), retrying..." << endl;
                    close(cfd);
                    sleep(1);
                }
            }
            if (!connected) {
                cout << "[DEBUG] Failed to connect after retries for peer " << i << endl;
                return 1;
            }
        } else {
            cout << "[DEBUG] Waiting for inbound connection from " << ctx.peers[i].remote_ip_str << endl;
            int cfd = accept(listenfd, nullptr, nullptr);
            if (cfd < 0) {
                cout << "[DEBUG] Accept failed for peer " << i << endl;
                return 1;
            }
            read(cfd, &ctx.peers[i].remote, sizeof(ctx.peers[i].remote));
            write(cfd, &ctx.peers[i].local, sizeof(ctx.peers[i].local));
            close(cfd);
        }
    }

    close(listenfd);
    cout << "[DEBUG] All-to-all exchange completed" << endl;
    return 0;
}

static int barrier_handshake(rdma_context &ctx, int phase) {
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(8081);
    bind(listenfd, (struct sockaddr *) &servaddr, sizeof(servaddr));
    listen(listenfd, ctx.peers.size() * 2);

    in_addr addrLocal;
    inet_aton(ctx.ip_str.c_str(), &addrLocal);
    for (int i = 0; i < (int) ctx.peers.size(); i++) {
        in_addr addrRemote;
        inet_aton(ctx.peers[i].remote_ip_str.c_str(), &addrRemote);
        bool smaller = (ntohl(addrLocal.s_addr) < ntohl(addrRemote.s_addr));
        if (smaller) {
            bool ok = false;
            for (int attempt = 0; attempt < 10; attempt++) {
                int cfd = socket(AF_INET, SOCK_STREAM, 0);
                sockaddr_in peer;
                memset(&peer, 0, sizeof(peer));
                peer.sin_family = AF_INET;
                peer.sin_addr.s_addr = addrRemote.s_addr;
                peer.sin_port = htons(8081);
                if (connect(cfd, (sockaddr *) &peer, sizeof(peer)) == 0) {
                    write(cfd, &phase, sizeof(phase));
                    int p2 = 0;
                    read(cfd, &p2, sizeof(p2));
                    close(cfd);
                    ok = true;
                    break;
                }
                close(cfd);
                sleep(1);
            }
            if (!ok) {
                close(listenfd);
                return 1;
            }
        } else {
            int cfd = accept(listenfd, nullptr, nullptr);
            int p1 = 0;
            read(cfd, &p1, sizeof(p1));
            write(cfd, &phase, sizeof(phase));
            close(cfd);
        }
    }
    close(listenfd);
    return 0;
}

void all_to_all_flow(rdma_context &ctx) {
    for (int i = 0; i < (int) ctx.peers.size(); i++) {
        memset(ctx.peers[i].data_write, 0, sizeof(ctx.peers[i].data_write));
        memset(ctx.peers[i].data_send, 0, sizeof(ctx.peers[i].data_send));
        memcpy(ctx.peers[i].data_write, ctx.message.c_str(), sizeof(ctx.peers[i].data_write) - 1);
        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.peers[i].send_mr->addr;
        ctx.sg_recv.length = sizeof(ctx.peers[i].data_send);
        ctx.sg_recv.lkey = ctx.peers[i].send_mr->lkey;
        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;
        ibv_post_recv(ctx.peers[i].write_qp, &ctx.wr_recv, &ctx.bad_wr_recv);
        memset(&ctx.sg_write, 0, sizeof(ctx.sg_write));
        ctx.sg_write.addr = (uintptr_t) ctx.peers[i].write_mr->addr;
        ctx.sg_write.length = sizeof(ctx.peers[i].data_write);
        ctx.sg_write.lkey = ctx.peers[i].write_mr->lkey;
        memset(&ctx.wr_write, 0, sizeof(ctx.wr_write));
        ctx.wr_write.sg_list = &ctx.sg_write;
        ctx.wr_write.num_sge = 1;
        ctx.wr_write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        ctx.wr_write.send_flags = IBV_SEND_SIGNALED;
        ctx.wr_write.imm_data = htonl(0xABCD);
        ctx.wr_write.wr.rdma.remote_addr = (uintptr_t) ctx.peers[i].remote.send_mr.addr;
        ctx.wr_write.wr.rdma.rkey = ctx.peers[i].remote.send_mr.rkey;
        ibv_post_send(ctx.peers[i].write_qp, &ctx.wr_write, &ctx.bad_wr_write);
        while (!ibv_poll_cq(ctx.peers[i].write_cq, 1, &ctx.wc)) {
        }
        cout << "[DEBUG] PHASE1 received from peer " << i << ": " << ctx.peers[i].data_send << endl;
    }
    barrier_handshake(ctx, 1);

    for (int i = 0; i < (int) ctx.peers.size(); i++) {
        memset(ctx.peers[i].data_write, 0, sizeof(ctx.peers[i].data_write));
        memset(ctx.peers[i].data_send, 0, sizeof(ctx.peers[i].data_send));
        string msg2 = ctx.message + " [Phase2]";
        memcpy(ctx.peers[i].data_write, msg2.c_str(), sizeof(ctx.peers[i].data_write) - 1);
        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.peers[i].send_mr->addr;
        ctx.sg_recv.length = sizeof(ctx.peers[i].data_send);
        ctx.sg_recv.lkey = ctx.peers[i].send_mr->lkey;
        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;
        ibv_post_recv(ctx.peers[i].write_qp, &ctx.wr_recv, &ctx.bad_wr_recv);
        memset(&ctx.sg_write, 0, sizeof(ctx.sg_write));
        ctx.sg_write.addr = (uintptr_t) ctx.peers[i].write_mr->addr;
        ctx.sg_write.length = sizeof(ctx.peers[i].data_write);
        ctx.sg_write.lkey = ctx.peers[i].write_mr->lkey;
        memset(&ctx.wr_write, 0, sizeof(ctx.wr_write));
        ctx.wr_write.sg_list = &ctx.sg_write;
        ctx.wr_write.num_sge = 1;
        ctx.wr_write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        ctx.wr_write.send_flags = IBV_SEND_SIGNALED;
        ctx.wr_write.imm_data = htonl(0xEEFF);
        ctx.wr_write.wr.rdma.remote_addr = (uintptr_t) ctx.peers[i].remote.send_mr.addr;
        ctx.wr_write.wr.rdma.rkey = ctx.peers[i].remote.send_mr.rkey;
        ibv_post_send(ctx.peers[i].write_qp, &ctx.wr_write, &ctx.bad_wr_write);
        while (!ibv_poll_cq(ctx.peers[i].write_cq, 1, &ctx.wc)) {
        }
        cout << "[DEBUG] PHASE2 received from peer " << i << ": " << ctx.peers[i].data_send << endl;
    }
    barrier_handshake(ctx, 2);
    cout << "[DEBUG] All-to-all flow (two-phase + barrier) completed" << endl;
}


void cleanup_rdma(rdma_context &ctx) {
    cout << "[DEBUG] Starting cleanup." << endl;

    // Destroy QPs, deregister MRs, and destroy CQs for each peer
    for (int i = 0; i < ctx.peers.size(); i++) {
        if (ctx.peers[i].send_qp) {
            ibv_destroy_qp(ctx.peers[i].send_qp);
            ctx.peers[i].send_qp = nullptr;
        }
        if (ctx.peers[i].write_qp) {
            ibv_destroy_qp(ctx.peers[i].write_qp);
            ctx.peers[i].write_qp = nullptr;
        }
        if (ctx.peers[i].send_mr) {
            ibv_dereg_mr(ctx.peers[i].send_mr);
            ctx.peers[i].send_mr = nullptr;
        }
        if (ctx.peers[i].write_mr) {
            ibv_dereg_mr(ctx.peers[i].write_mr);
            ctx.peers[i].write_mr = nullptr;
        }
        if (ctx.peers[i].send_cq) {
            ibv_destroy_cq(ctx.peers[i].send_cq);
            ctx.peers[i].send_cq = nullptr;
        }
        if (ctx.peers[i].write_cq) {
            ibv_destroy_cq(ctx.peers[i].write_cq);
            ctx.peers[i].write_cq = nullptr;
        }
    }

    // Deallocate the PD
    if (ctx.pd) {
        ibv_dealloc_pd(ctx.pd);
        ctx.pd = nullptr;
    }

    // Close the device
    if (ctx.context) {
        ibv_close_device(ctx.context);
        ctx.context = nullptr;
    }

    // Free the device list
    if (ctx.dev_list) {
        ibv_free_device_list(ctx.dev_list);
        ctx.dev_list = nullptr;
    }

    cout << "[DEBUG] Cleanup complete." << endl;
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
    all_to_all_exchange_data(ctx);

    modify_qps_rtr(ctx);
    modify_qps_rts(ctx);

    all_to_all_flow(ctx);

    sleep(1);
    cleanup_rdma(ctx);
    sleep(1);

    return 0;
}
