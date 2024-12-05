import threading

class KeyValueStore:
    def __init__(self):
        # Initialize the store with a thread-safe dictionary
        self.store = {}
        self.lock = threading.Lock()

    def create_context(self, context_id):
        with self.lock:
            if context_id not in self.store:
                self.store[context_id] = ""
                print(f"NEW CONTEXT {context_id}")
            else:
                print(f"Context {context_id} already exists.")

    def add_query(self, context_id, query):
        with self.lock:
            if context_id in self.store:
                self.store[context_id] += f"Query: {query}\n"
                updated_context = self.store[context_id]
                print(f"NEW QUERY on {context_id} with {updated_context}")
            else:
                print(f"Context {context_id} does not exist.")

    def add_answer(self, context_id, answer):
        with self.lock:
            if context_id in self.store:
                self.store[context_id] += f"Answer: {answer}\n"
                print(f"CHOSEN ANSWER on {context_id} with {answer}")
            else:
                print(f"Context {context_id} does not exist.")

    def get_context(self, context_id):
        with self.lock:
            return self.store.get(context_id, None)

    def get_all_contexts(self):
        with self.lock:
            return self.store.copy()
