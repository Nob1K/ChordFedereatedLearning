struct node {
    1: string ip,
    2: i32 port
}

service supernode {
    i32 request_join(1:i32 node_port),
    bool confirm_join(),
    node get_node()

}