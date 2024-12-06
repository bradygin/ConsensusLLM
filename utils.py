def generate_ballot_number(current_ballot, node_id):
    seq_num, pid, op_num = current_ballot
    seq_num += 1
    return (seq_num, node_id, op_num)
