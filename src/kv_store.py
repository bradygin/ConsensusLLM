class KeyValueStore:
    def __init__(self):
        self.contexts = {}

    def create_context(self, cid):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        print(f"NEW CONTEXT {cid}")
    
    def add_query(self, cid, query_str):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        self.contexts[cid] += f"Query: {query_str}\n"
        print(f"NEW QUERY on {cid} with {self.contexts[cid]}")

    def add_answer(self, cid, answer_str):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        self.contexts[cid] += f"Answer: {answer_str}\n"
        print(f"CHOSEN ANSWER on {cid} with {answer_str}")

    def view_context(self, cid):
        if cid in self.contexts:
            print(self.contexts[cid])
        else:
            print(f"Context {cid} not found.")

    def view_all(self):
        for cid, ctx in self.contexts.items():
            print(f"view {cid}")
            print(ctx)

    def get_full_context(self, cid):
        # Return the full context string for cid
        return self.contexts.get(cid, "")
