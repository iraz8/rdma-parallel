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

struct rdma_context {
    bool server = false;
    int num_devices = 0;
    int ret = 0;
    uint32_t gidIndex = 0;
    vector<string> ip_strs, remote_ip_strs;
    string dev_str;
    char data_send[100], data_write[100];

    ibv_device **dev_list = nullptr;
    ibv_context *context = nullptr;
    ibv_pd *pd = nullptr;
    ibv_cq *send_cq = nullptr;
    ibv_cq *write_cq = nullptr;
    ibv_qp_init_attr qp_init_attr{};
    vector<ibv_qp *> send_qps;
    vector<ibv_qp *> write_qps;
    ibv_qp_attr qp_attr{};
    ibv_port_attr port_attr{};
    vector<device_info> local{};
    vector<device_info> remote{};
    ibv_gid_entry gidEntries[255];
    ibv_sge sg_send{}, sg_write{}, sg_recv{};
    ibv_send_wr wr_send{}, *bad_wr_send = nullptr, wr_write{}, *bad_wr_write = nullptr;
    ibv_recv_wr wr_recv{}, *bad_wr_recv = nullptr;
    vector<ibv_mr *> send_mrs;
    vector<ibv_mr *> write_mrs;
    ibv_mr remote_write_mr{};
    ibv_wc wc{};
    int num_nodes = 0;
};

int receive_data_all(rdma_context &ctx) {
    int sockfd, connfd;
    struct sockaddr_in servaddr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        cerr << "[receive_data_all] Socket creation failed: " << strerror(errno) << endl;
        return 1;
    }

    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        cerr << "[receive_data_all] setsockopt failed: " << strerror(errno) << endl;
        close(sockfd);
        return 1;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(8080);

    if (bind(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) != 0) {
        cerr << "[receive_data_all] Bind failed: " << strerror(errno) << endl;
        close(sockfd);
        return 1;
    }

    if (listen(sockfd, ctx.num_nodes * 2) != 0) {
        cerr << "[receive_data_all] Listen failed: " << strerror(errno) << endl;
        close(sockfd);
        return 1;
    }

    cout << "[receive_data_all] Server listening on port 8080." << endl;

    int accepted_connections = 0;
    while (accepted_connections < ctx.num_nodes) {
        connfd = accept(sockfd, NULL, NULL);
        if (connfd < 0) {
            cerr << "[receive_data_all] Accept failed: " << strerror(errno) << endl;
            close(sockfd);
            return 1;
        }
        cout << "[receive_data_all] Accepted connection." << endl;

        if (read(connfd, &ctx.remote[accepted_connections], sizeof(ctx.remote[accepted_connections])) <= 0) {
            cerr << "[receive_data_all] Failed to receive data: " << strerror(errno) << endl;
            close(connfd);
            continue;
        }
        accepted_connections++;
        close(connfd);
    }
    close(sockfd);
    return 0;
}


int send_data_all(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        int sockfd;
        struct sockaddr_in servaddr;
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            cerr << "[send_data_all] Socket creation failed for node " << i << ": " << strerror(errno) << endl;
            continue;
        }
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = inet_addr(ctx.remote_ip_strs[i].c_str());
        servaddr.sin_port = htons(8080);

        cout << "[send_data_all] Attempting to connect to " << ctx.remote_ip_strs[i] << " on port 8080." << endl;
        int retry_count = 30;
        while (connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) != 0 && retry_count > 0) {
            cerr << "[send_data_all] Connection retrying... (" << retry_count << " attempts left)" << endl;
            sleep(1);
            retry_count--;
        }

        if (retry_count == 0) {
            cerr << "[send_data_all] Connection failed for node " << i << ". Skipping." << endl;
            close(sockfd);
            continue;
        }

        if (write(sockfd, &ctx.local[i], sizeof(ctx.local[i])) <= 0) {
            cerr << "[send_data_all] Write failed for node " << i << ": " << strerror(errno) << endl;
            close(sockfd);
            continue;
        }
        close(sockfd);
    }
    return 0;
}


void parse_program_options(int argc, char *argv[], rdma_context &ctx) {
    auto flags = IBV_ACCESS_LOCAL_WRITE |
                 IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_READ;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
            ("help", "show possible options")
            ("dev", boost::program_options::value<string>(), "rdma device to use")
            ("src_ip", boost::program_options::value<vector<string> >()->multitoken(), "source IPs (one per node)")
            ("dst_ip", boost::program_options::value<vector<string> >()->multitoken(), "destination IPs (one per node)")
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

    if (vm.count("src_ip")) {
        ctx.ip_strs = vm["src_ip"].as<vector<string> >();
    } else {
        cerr << "the --src_ip argument is required" << endl;
    }

    if (vm.count("dst_ip")) {
        ctx.remote_ip_strs = vm["dst_ip"].as<vector<string> >();
    } else {
        cerr << "the --dst_ip argument is required" << endl;
    }

    ctx.num_nodes = ctx.ip_strs.size();

    if (vm.count("server"))
        ctx.server = true;
    ctx.local.resize(ctx.num_nodes);
    ctx.remote.resize(ctx.num_nodes);
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
    // create a CQ (completion queue) for the send operations, using ibv_create_cq
    ctx.send_cq = ibv_create_cq(ctx.context, 0x10, nullptr, nullptr, 0);
    if (!ctx.send_cq) {
        cerr << "ibv_create_cq - send - failed: " << strerror(errno) << endl;
        ibv_dealloc_pd(ctx.pd);
        exit(1);
    }

    // create a CQ for the write operations, using ibv_create_cq
    ctx.write_cq = ibv_create_cq(ctx.context, 0x10, nullptr, nullptr, 0);
    if (!ctx.write_cq) {
        cerr << "ibv_create_cq - recv - failed: " << strerror(errno) << endl;
        ibv_destroy_cq(ctx.send_cq);
        exit(1);
    }
}

void create_qps(rdma_context &ctx) {
    ctx.send_qps.resize(ctx.num_nodes);
    ctx.write_qps.resize(ctx.num_nodes);
    memset(&ctx.qp_init_attr, 0, sizeof(ctx.qp_init_attr));

    ctx.qp_init_attr.recv_cq = ctx.send_cq;
    ctx.qp_init_attr.send_cq = ctx.send_cq;

    ctx.qp_init_attr.qp_type = IBV_QPT_RC;
    ctx.qp_init_attr.sq_sig_all = 1;

    ctx.qp_init_attr.cap.max_send_wr = 5;
    ctx.qp_init_attr.cap.max_recv_wr = 5;
    ctx.qp_init_attr.cap.max_send_sge = 1;
    ctx.qp_init_attr.cap.max_recv_sge = 1;

    for (int i = 0; i < ctx.num_nodes; i++) {
        // create a QP (queue pair) for the send operations, using ibv_create_qp
        ctx.send_qps[i] = ibv_create_qp(ctx.pd, &ctx.qp_init_attr);
        if (!ctx.send_qps[i]) {
            cerr << "ibv_create_qp failed: " << strerror(errno) << endl;
            ibv_destroy_cq(ctx.write_cq);
            exit(1);
        }

        ctx.qp_init_attr.recv_cq = ctx.write_cq;
        ctx.qp_init_attr.send_cq = ctx.write_cq;

        // create a QP for the write operations, using ibv_create_qp
        ctx.write_qps[i] = ibv_create_qp(ctx.pd, &ctx.qp_init_attr);
        if (!ctx.write_qps[i]) {
            cerr << "ibv_create_qp failed: " << strerror(errno) << endl;
            ibv_destroy_qp(ctx.send_qps[i]);
            exit(1);
        }
    }
}

void modify_qps_init(rdma_context &ctx) {
    memset(&ctx.qp_attr, 0, sizeof(ctx.qp_attr));

    ctx.qp_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    ctx.qp_attr.port_num = 1;
    ctx.qp_attr.pkey_index = 0;
    ctx.qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ;

    for (int i = 0; i < ctx.num_nodes; i++) {
        // move both QPs in the INIT state, using ibv_modify_qp
        ctx.ret = ibv_modify_qp(ctx.send_qps[i], &ctx.qp_attr,
                                IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - INIT - failed for node: " << i << " error: " << strerror(ctx.ret) << endl;
            ibv_destroy_qp(ctx.write_qps[i]);
            exit(1);
        }

        ctx.ret = ibv_modify_qp(ctx.write_qps[i], &ctx.qp_attr,
                                IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - INIT - failed for node: " << i << " error: " << strerror(ctx.ret) << endl;
            ibv_destroy_qp(ctx.write_qps[i]);
            exit(1);
        }
    }
}

void find_gid(rdma_context &ctx) {
    ctx.local.resize(ctx.num_nodes);
    ibv_query_port(ctx.context, 1, &ctx.port_attr);
    ibv_query_gid_table(ctx.context, ctx.gidEntries, ctx.port_attr.gid_tbl_len, 0);

    for (int i = 0; i < ctx.num_nodes; ++i) {
        bool gid_found = false;
        for (auto &entry: ctx.gidEntries) {
            if (entry.gid_type != IBV_GID_TYPE_ROCE_V2)
                continue;

            in6_addr addr;
            memcpy(&addr, &entry.gid.global, sizeof(addr));

            char interface_id[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, &addr, interface_id, INET6_ADDRSTRLEN);

            if (strncmp(ctx.ip_strs[i].c_str(), interface_id + strlen("::ffff:"), INET_ADDRSTRLEN) == 0) {
                ctx.local[i].gid = entry.gid;
                gid_found = true;
                break;
            }
        }
        if (!gid_found) {
            cerr << "GID not found for node " << i << endl;
            exit(1);
        }
    }
}


void register_memory(rdma_context &ctx) {
    ctx.send_mrs.resize(ctx.num_nodes);
    ctx.write_mrs.resize(ctx.num_nodes);
    auto flags = IBV_ACCESS_LOCAL_WRITE |
                 IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_READ;

    for (int i = 0; i < ctx.num_nodes; i++) {
        // register the "data_send" and "data_write" buffers for RDMA operations, using ibv_reg_mr;
        // store the resulting mrs in send_mr and write_mr
        ctx.send_mrs[i] = ibv_reg_mr(ctx.pd, ctx.data_send, sizeof(ctx.data_send), flags);
        if (!ctx.send_mrs[i]) {
            cerr << "ibv_reg_mr failed for node: " << i << " error: " << strerror(errno) << endl;
            for (int j = 0; j < i; j++) {
                ibv_destroy_qp(ctx.write_qps[j]);
            }
            exit(1);
        }

        ctx.write_mrs[i] = ibv_reg_mr(ctx.pd, ctx.data_write, sizeof(ctx.data_write), flags);
        if (!ctx.write_mrs[i]) {
            cerr << "ibv_reg_mr failed for node: " << i << " error: " << strerror(errno) << endl;
            for (int j = 0; j < i; j++) {
                ibv_dereg_mr(ctx.send_mrs[j]);
            }
            exit(1);
        }

        memcpy(&ctx.local[i].write_mr, ctx.write_mrs[i], sizeof(ctx.local[i].write_mr));
        ctx.local[i].send_qp_num = ctx.send_qps[i]->qp_num;
        ctx.local[i].write_qp_num = ctx.write_qps[i]->qp_num;
    }
}

void exchange_data_info(rdma_context &ctx) {
    if (ctx.server) {
        if (receive_data_all(ctx) != 0) {
            cerr << "Error receiving data from all nodes." << endl;
            for (auto &mr: ctx.write_mrs) {
                ibv_dereg_mr(mr);
            }
            exit(1);
        }
        if (send_data_all(ctx) != 0) {
            cerr << "Error sending data to all nodes." << endl;
            for (auto &mr: ctx.write_mrs) {
                ibv_dereg_mr(mr);
            }
            exit(1);
        }
    } else {
        if (send_data_all(ctx) != 0) {
            cerr << "Error sending data to all nodes." << endl;
            for (auto &mr: ctx.write_mrs) {
                ibv_dereg_mr(mr);
            }
            exit(1);
        }
        if (receive_data_all(ctx) != 0) {
            cerr << "Error receiving data from all nodes." << endl;
            for (auto &mr: ctx.write_mrs) {
                ibv_dereg_mr(mr);
            }
            exit(1);
        }
    }
}

void modify_qps_rtr(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        memset(&ctx.qp_attr, 0, sizeof(ctx.qp_attr));

        ctx.qp_attr.path_mtu = ctx.port_attr.active_mtu;
        ctx.qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        ctx.qp_attr.rq_psn = 0;
        ctx.qp_attr.max_dest_rd_atomic = 1;
        ctx.qp_attr.min_rnr_timer = 0;
        ctx.qp_attr.ah_attr.is_global = 1;
        ctx.qp_attr.ah_attr.sl = 0;
        ctx.qp_attr.ah_attr.src_path_bits = 0;
        ctx.qp_attr.ah_attr.port_num = 1;

        memcpy(&ctx.qp_attr.ah_attr.grh.dgid, &ctx.remote[i].gid, sizeof(ctx.remote[i].gid));
        ctx.qp_attr.ah_attr.grh.flow_label = 0;
        ctx.qp_attr.ah_attr.grh.hop_limit = 5;
        ctx.qp_attr.ah_attr.grh.sgid_index = ctx.gidIndex;
        ctx.qp_attr.ah_attr.grh.traffic_class = 0;

        ctx.qp_attr.ah_attr.dlid = 1;
        ctx.qp_attr.dest_qp_num = ctx.remote[i].send_qp_num;

        // move the send QP into the RTR state, using ibv_modify_qp
        ctx.ret = ibv_modify_qp(ctx.send_qps[i], &ctx.qp_attr, IBV_QP_STATE | IBV_QP_AV |
                                                               IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                                                               IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);

        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - RTR - failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }

        ctx.qp_attr.dest_qp_num = ctx.remote[i].write_qp_num;

        // move the write QP into the RTR state, using ibv_modify_qp
        ctx.ret = ibv_modify_qp(ctx.write_qps[i], &ctx.qp_attr, IBV_QP_STATE | IBV_QP_AV |
                                                                IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                                                                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);

        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - RTR - failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
    }
}

void modify_qps_rts(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        ctx.qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        ctx.qp_attr.timeout = 0;
        ctx.qp_attr.retry_cnt = 7;
        ctx.qp_attr.rnr_retry = 7;
        ctx.qp_attr.sq_psn = 0;
        ctx.qp_attr.max_rd_atomic = 0;

        // move the send and write QPs into the RTS state, using ibv_modify_qp
        ctx.ret = ibv_modify_qp(ctx.send_qps[i], &ctx.qp_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                                                               IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                                                               IBV_QP_MAX_QP_RD_ATOMIC);
        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - RTS - failed for node: " << i << " error: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }

        ctx.ret = ibv_modify_qp(ctx.write_qps[i], &ctx.qp_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                                                                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                                                                IBV_QP_MAX_QP_RD_ATOMIC);
        if (ctx.ret != 0) {
            cerr << "ibv_modify_qp - RTS - failed for node: " << i << " error: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
    }
}

void server_flow(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        memset(ctx.data_send, 0, sizeof(ctx.data_send));
        memset(ctx.data_write, 0, sizeof(ctx.data_write));

        memcpy(ctx.data_write, "Hello, but with write", 21);
        // initialise sg_write with the write mr address, size and lkey
        memset(&ctx.sg_write, 0, sizeof(ctx.sg_write));
        ctx.sg_write.addr = (uintptr_t) ctx.write_mrs[i]->addr;
        ctx.sg_write.length = sizeof(ctx.data_write);
        ctx.sg_write.lkey = ctx.write_mrs[i]->lkey;
        // create a work request, with the Write With Immediate operation
        memset(&ctx.wr_write, 0, sizeof(ctx.wr_write));
        ctx.wr_write.wr_id = 0;
        ctx.wr_write.sg_list = &ctx.sg_write;
        ctx.wr_write.num_sge = 1;
        ctx.wr_write.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        ctx.wr_write.send_flags = IBV_SEND_SIGNALED;
        ctx.wr_write.imm_data = htonl(0x1234);
        // fill the wr.rdma field of wr_write with the remote address and key
        ctx.wr_write.wr.rdma.remote_addr = (uintptr_t) ctx.remote[i].write_mr.addr;
        ctx.wr_write.wr.rdma.rkey = ctx.remote[i].write_mr.rkey;
        // post the work request, using ibv_post_send
        ctx.ret = ibv_post_send(ctx.write_qps[i], &ctx.wr_write, &ctx.bad_wr_write);
        if (ctx.ret != 0) {
            cerr << "ibv_post_send failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
        // initialise sg_send with the send mr address, size and lkey
        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.send_mrs[i]->addr;
        ctx.sg_recv.length = sizeof(ctx.data_send);
        ctx.sg_recv.lkey = ctx.send_mrs[i]->lkey;
        // create a receive work request
        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.wr_id = 0;
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;
        // post the receive work request, using ibv_post_recv, for the send QP
        ctx.ret = ibv_post_recv(ctx.send_qps[i], &ctx.wr_recv, &ctx.bad_wr_recv);
        if (ctx.ret != 0) {
            cerr << "ibv_post_recv failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
        // poll send_cq, using ibv_poll_cq, until it returns different than 0
        ctx.ret = 0;
        do {
            ctx.ret = ibv_poll_cq(ctx.send_cq, 1, &ctx.wc);
        } while (ctx.ret == 0);
        // check the wc (work completion) structure status;
        // return error on anything different than ibv_wc_status::IBV_WC_SUCCESS
        if (ctx.wc.status != ibv_wc_status::IBV_WC_SUCCESS) {
            cerr << "ibv_poll_cq failed: " << ibv_wc_status_str(ctx.wc.status) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
        cout << ctx.data_send << endl;
    }
}

void client_flow(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        memcpy(ctx.data_send, "Hello", 5);
        // initialise sg_write with the write mr address, size and lkey
        memset(&ctx.sg_recv, 0, sizeof(ctx.sg_recv));
        ctx.sg_recv.addr = (uintptr_t) ctx.write_mrs[i]->addr;
        ctx.sg_recv.length = sizeof(ctx.data_write);
        ctx.sg_recv.lkey = ctx.write_mrs[i]->lkey;
        memset(&ctx.wr_recv, 0, sizeof(ctx.wr_recv));
        ctx.wr_recv.wr_id = 0;
        ctx.wr_recv.sg_list = &ctx.sg_recv;
        ctx.wr_recv.num_sge = 1;
        // post a receive work request, using ibv_post_recv, for the write QP
        ctx.ret = ibv_post_recv(ctx.write_qps[i], &ctx.wr_recv, &ctx.bad_wr_recv);
        if (ctx.ret != 0) {
            cerr << "ibv_post_recv failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
        // poll write_cq, using ibv_poll_cq, until it returns different than 0
        ctx.ret = 0;
        do {
            ctx.ret = ibv_poll_cq(ctx.write_cq, 1, &ctx.wc);
        } while (ctx.ret == 0);
        // check the wc (work completion) structure status;
        //         return error on anything different than ibv_wc_status::IBV_WC_SUCCESS
        if (ctx.wc.status != ibv_wc_status::IBV_WC_SUCCESS) {
            cerr << "ibv_poll_cq failed: " << ibv_wc_status_str(ctx.wc.status) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
        cout << ctx.data_write << endl;
        // initialise sg_send with the send mr address, size and lkey
        memset(&ctx.sg_send, 0, sizeof(ctx.sg_send));
        ctx.sg_send.addr = (uintptr_t) ctx.send_mrs[i]->addr;
        ctx.sg_send.length = sizeof(ctx.data_send);
        ctx.sg_send.lkey = ctx.send_mrs[i]->lkey;
        // create a work request, with the RDMA Send operation
        memset(&ctx.wr_send, 0, sizeof(ctx.wr_send));
        ctx.wr_send.wr_id = 0;
        ctx.wr_send.sg_list = &ctx.sg_send;
        ctx.wr_send.num_sge = 1;
        ctx.wr_send.opcode = IBV_WR_SEND;
        ctx.wr_send.send_flags = IBV_SEND_SIGNALED;
        // post the work request, using ibv_post_send
        ctx.ret = ibv_post_send(ctx.send_qps[i], &ctx.wr_send, &ctx.bad_wr_send);
        if (ctx.ret != 0) {
            cerr << "ibv_post_recv failed: " << strerror(ctx.ret) << endl;
            ibv_dereg_mr(ctx.write_mrs[i]);
            exit(1);
        }
    }
}

void cleanup_resources(rdma_context &ctx) {
    for (int i = 0; i < ctx.num_nodes; i++) {
        if (ctx.send_mrs[i])
            ibv_dereg_mr(ctx.send_mrs[i]);
        if (ctx.write_mrs[i])
            ibv_dereg_mr(ctx.write_mrs[i]);
        if (ctx.send_qps[i])
            ibv_destroy_qp(ctx.send_qps[i]);
        if (ctx.write_qps[i])
            ibv_destroy_qp(ctx.write_qps[i]);
    }
    if (ctx.send_cq)
        ibv_destroy_cq(ctx.send_cq);
    if (ctx.write_cq)
        ibv_destroy_cq(ctx.write_cq);
    if (ctx.pd)
        ibv_dealloc_pd(ctx.pd);
    if (ctx.context)
        ibv_close_device(ctx.context);
    if (ctx.dev_list)
        ibv_free_device_list(ctx.dev_list);
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
    try {
        exchange_data_info(ctx);
        modify_qps_rtr(ctx);
        modify_qps_rts(ctx);
        if (ctx.server) {
            server_flow(ctx);
        } else {
            client_flow(ctx);
        }
    } catch (const std::exception &e) {
        cerr << "An error occurred: " << e.what() << endl;
        cleanup_resources(ctx);
        return 1;
    }

    cleanup_resources(ctx);
    return 0;
}
