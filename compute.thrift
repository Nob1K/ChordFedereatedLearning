struct node {
    1: string ip,
    2: i32 port
}

struct weights {
    1: list<list<double>> w,
    2: list<list<double>> v,
    3: i32 status // 0 for normal, 1 for wait, -1 for error
}

service compute {
    oneway void put_data(1:string filename),
    weights get_model(1:string filename),
    void fix_fingers(1: i32 start_id),
    void print_info(),
    
}