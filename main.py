import socket
import threading
import pickle
import time
import selectors
from http.server import BaseHTTPRequestHandler, HTTPServer
from utils import find_next_empty, is_valid, split_array_in_middle
import argparse
import json
from sudoku import Sudoku
import queue

class DHTNode(threading.Thread):
    def __init__(self, host, port, http_port, dht=None, handicap=1):
        super().__init__()
        self.starttime = time.time()
        self.host = host
        self.port = port
        self.dht = dht
        self.http_port = http_port
        self.predecessor = None
        self.neighbor = None
        self.neighborfree = False
        self.task = []
        self.selector = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(5)
        self.sock.setblocking(False)
        self.selector.register(self.sock, selectors.EVENT_READ, self.handle_request)
        self.running = True
        self.inside_dht = False
        self.task_queue = queue.Queue()
        self.validations = 0
        self.solved_count = 0 
        self.handicap = handicap
        self.solution = None
        self.http_server = threading.Thread(target=self.run_http_server)
        self.http_server.start()



    def run_http_server(self):
        """Start the HTTP server."""
        server_address = (self.host, self.http_port)
        httpd = HTTPServer(server_address, SudokuHandler)
        httpd.dht_node = self
        print(f'HTTP server started on {self.http_port}')
        httpd.serve_forever()


    def send_data(self, data, addr):
        """Send data to another node."""
        if addr == ('127.0.0.1', 5001):
            print(f"I send data to {addr}")
        self.sock.sendto(pickle.dumps(data), addr)


    def non_blocking_receive(self):
        try:
            data, addr = self.sock.recvfrom(1024)  # Assuming the socket is set to non-blocking
            return pickle.loads(data), addr
        except socket.error as e:
            if e.errno != socket.errno.EWOULDBLOCK:
                print(f"Socket error: {e}")
            return None, None  # No data received
        
    def receive_data(self):
        """Receive data from other nodes."""
        # print("I try to receive data")
        try:
            data, addr = self.sock.recvfrom(1024)
        except socket.timeout:
            return None, None
        
        if len(data) == 0:
            return None, addr
        return pickle.loads(data), addr
        # try:
        #     data, addr = sock.recvfrom(1024)
        #     if data:
        #         data = pickle.loads(data)
        #         self.handle_request(data, addr)
        # except socket.error as e:
        #     print(f"Socket error: {e}")
        # except pickle.PickleError as pe:
        #     print(f"Pickle error: {pe}")

    def run(self):
        """Main loop to receive and handle data."""
        print(f"MOin from port {self.port}")
        if not self.inside_dht:
            if self.dht:
                print(f"I try to join {self.host_port_tuple()}")
                join_request = {
                    'method': 'JOIN_REQ',
                }
                self.send_data(join_request, self.dht)
                # data, addr = self.non_blocking_receive()
                # if data:
                #     print(f"JOIN_LOOP: Received message: {data} from {addr}")
                    # if data['method'] == 'JOIN_RES':
                    #     new_predecessor = data['predecessor']
                    #     new_neighbor = data['neighbor']

                    #     # Update the predecessor to the sender of the JOIN_RES message
                    #     self.predecessor = new_predecessor

                    #     # Update the neighbor according to the information received
                    #     self.neighbor = new_neighbor

                    #     # Update the dht status
                    #     self.inside_dht = True

                    #     if self.predecessor:
                    #         print(f"Node {self.port} asks for work")
                    #         self.send_data({
                    #         'method': 'NEEDWORK',
                    #         }, self.predecessor)
            else:
                self.inside_dht = True
        while self.running:
            events = self.selector.select(timeout=None)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)


    def stop(self):
        """Stop the node."""
        print(f"Node {self.port} stops now")
        if self.task:
            self.task_queue.put(self.task)
            self.task = []
        while not self.task_queue.empty() and self.neighbor:
            next_task = self.task_queue.get()
            self.send_data(next_task, self.neighbor)

        # Update neighbor and predecessor before stopping
        if self.neighbor and self.predecessor:
            self.send_data({
                'method': 'UPDATE_NEIGHBOR',
                'neighbor': self.neighbor
            }, self.predecessor)
            self.send_data({
                'method': 'UPDATE_PREDECESSOR',
                'predecessor': self.predecessor
            }, self.neighbor)

        
        self.running = False
        self.selector.unregister(self.sock)
        self.sock.close()
        self.selector.close()

    def handle_request(self, sock, mask):
        try:
            data, addr = self.receive_data()
            if data:
                self.handleMessage(data, addr)
        except Exception as e:
            print(f"Error receiving data: {e} on {self.port}")


         
    
    def handleMessage(self, data, addr):
        # print(f"Received message: {data} from {addr}")
        if data['method'] == 'TASK':
            # print(f"I got new work on node {self.port}")
            if self.task != []:
                self.task_queue.put(data)
                print(f"I got a task but I already have sth todo {self.port}")
                return
            else:
                self.task_queue.put(data)
            self.task = data
            while not self.task_queue.empty():
                next_task = self.task_queue.get()
                self.task = next_task
                possible_solutions = self.task['range']
                sudoku = self.task['sudoku']
                self.perform_solving(sudoku, possible_solutions)

            if self.predecessor:
                # print(f"Node {self.port} asks for work")
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                # print(f"I ask for work after solving Node {self.port}")
            self.task = []
            
            

        elif data['method'] == 'NEEDWORK':
            self.neighborfree = True
            # print(f"Node {self.port} got it´s neighbor free")

        elif data['method'] == 'JOIN_REQ':
            if self.neighbor:
                old_neighbor = self.neighbor
            else:
                old_neighbor = self.host_port_tuple()
            incoming_request = addr

            # Update current node's neighbor to be the new node
            self.neighbor = incoming_request

            # Inform the old neighbor about its new predecessor (the new node)
            self.send_data({
                'method': 'UPDATE_PREDECESSOR',
                'predecessor': incoming_request,
            }, old_neighbor)

            # Inform the incoming request about its predecessor and successor
            self.send_data({
                'method': 'JOIN_RES',
                'predecessor': self.host_port_tuple(),
                'neighbor': old_neighbor,
            }, incoming_request)
            self.neighborfree = False


        elif data['method'] == 'JOIN_RES':
            print("I got a response")
            new_predecessor = data['predecessor']
            new_neighbor = data['neighbor']

            # Update the predecessor to the sender of the JOIN_RES message
            self.predecessor = new_predecessor

            # Update the neighbor according to the information received
            self.neighbor = new_neighbor

            # Update the dht status
            self.inside_dht = True

            if self.predecessor and self.task == []:
                # print(f"Node {self.port} asks for work")
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                # print(f"I ask for work because i got a join response {self.port}")

            self.neighborfree = False

        elif data['method'] == 'UPDATE_PREDECESSOR':
            new_predecessor = data['predecessor']
            self.predecessor = new_predecessor

            if self.predecessor and self.task == []:
                # print(f"Node {self.port} asks for work")
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                # print(f"I ask for work in predecessor and my task is {self.task} and i am node {self.port}")4
        
        elif data['method'] == 'UPDATE_NEIGHBOR':
            new_neighbor = data['neighbor']
            self.neighbor = new_neighbor

        elif data['method'] == 'SOLUTION_FOUND':
            print(f"Node {data['node']} found solution")
            print(f"With {self.validations} validations on {self.port}")
            if data['node'] != self.host_port_tuple():
                self.send_data(data, self.predecessor)
            self.task_queue.empty()
            self.task = []
            # self.stop()

        elif data['method'] == 'STOP':             
            self.stop()

    def host_port_tuple(self):
        return (self.host, self.port)

    def perform_counting(self, start, end):
        """Counts from start to end and prints each number."""
        for i in range(start, end + 1):
            print(f"Node {self.port} counting: {i}")
            time.sleep(1)  # Simulate time taken for counting

    def perform_solving(self, grid, arr):
        # print(f"Node {self.port} starts solving with range: {arr}")
        solution = self.solve_sudoku(grid, arr)
        # print(f"Node {self.port} found solution: {solution}")
        if solution == True:
            self.solved_count +=1
            self.solution= grid
            end_time = time.time()  # End timing after the algorithm completes
            elapsed_time = end_time - self.starttime  # Calculate the elapsed time
            print(f"Calculated time for solving: {elapsed_time}")
            if self.predecessor:
                self.send_data({
                    'method': 'SOLUTION_FOUND',
                    'solution': grid,  # You might need to serialize the grid here
                    'node': self.host_port_tuple(),
                }, self.predecessor)

    
    def solve_sudoku(self, puzzle, arr=range(1, 10)):
        # solve sudoku using backtracking!
        # our puzzle is a list of lists, where each inner list is a row in our sudoku puzzle
        # return whether a solution exists
        # mutates puzzle to be the solution (if solution exists)
        # Check if neighbor is free and give him work
        if self.task == []:
            return False
        data, addr = self.non_blocking_receive()
        if data:
            # Handle the message
            self.handleMessage(data, addr)

        if self.neighbor and self.neighborfree:
            # print(f"Node {self.port} sends half to {self.neighbor}")
            # Delegate the rest to the neighbor
            if len(arr) >1:
                first_half, arr = split_array_in_middle(arr)
                # print(first_half, arr)
                self.send_data({
                    'method': 'TASK',
                    'sudoku': puzzle,
                    'range': first_half
                }, self.neighbor)
                self.neighborfree = False
            #self.perform_counting(start_point, end_point)
            # print(f"Node {self.port} checks with range: {arr}")

        # update validations
        self.validations +=1
        # step 1: choose somewhere on the puzzle to make a guess
        row, col = find_next_empty(puzzle)

        # step 1.1: if there's nowhere left, then we're done because we only allowed valid inputs
        if row is None:  # this is true if our find_next_empty function returns None, None
            return True 
        
        # step 2: if there is a place to put a number, then make a guess between 1 and 9
        for guess in arr: # range(1, 10) is 1, 2, 3, ... 9
            time.sleep(0.01 * self.handicap)
            # step 3: check if this is a valid guess
            if is_valid(puzzle, guess, row, col):
                # step 3.1: if this is a valid guess, then place it at that spot on the puzzle
                self.validations +=1
                puzzle[row][col] = guess
                # step 4: then we recursively call our solver!
                if self.solve_sudoku(puzzle):
                    return True
            
            # step 5: it not valid or if nothing gets returned true, then we need to backtrack and try a new number
            puzzle[row][col] = 0

        # step 6: if none of the numbers that we try work, then this puzzle is UNSOLVABLE!!
        return False

class SudokuHandler(BaseHTTPRequestHandler):
    def do_POST(self):
            if self.path == '/solve':
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                puzzle = json.loads(post_data)['sudoku']
                sudoku = Sudoku(puzzle)
                self.server.dht_node.send_data({'method': 'TASK','sudoku': sudoku.grid, 'range': range(1, 10)}, (self.server.dht_node.host,self.server.dht_node.port ))
                
                while self.server.dht_node.solution == None:
                    time.sleep(0.1)

                solution = self.server.dht_node.solution
       

                self.send_response(201)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.server.dht_node.solution = None
                self.wfile.write(json.dumps({"solution": solution}).encode('utf-8'))


    def do_GET(self):
        if self.path == '/stats':
            stats = {
                "all": {
                    "solved": self.server.dht_node.solved_count,
                    "validations": self.server.dht_node.validations
                },
                "nodes": [
                    {
                        "address": f"{self.server.dht_node.host}:{self.server.dht_node.port}",
                        "validations": self.server.dht_node.validations
                    }
                ]
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(stats).encode('utf-8'))

        elif self.path == '/network':
            network = {
                "node": f"{self.server.dht_node.host}:{self.server.dht_node.port}",
                "predecessor": self.server.dht_node.predecessor,
                "neighbor": self.server.dht_node.neighbor
            }
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(network).encode('utf-8'))

def run(server_class=HTTPServer, handler_class=SudokuHandler, port=8000):
        server_address = ('', port)
        httpd = server_class(server_address, handler_class)
        print(f'Servidor HTTP iniciado na porta {port}')
        httpd.serve_forever()


if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Distributed Sudoku Solver Node")
        parser.add_argument('-p', '--port', type=int, required=True, help='HTTP port for the node')
        parser.add_argument('-s', '--p2p-port', type=int, required=True, help='P2P port for the node')
        parser.add_argument('-a', '--anchor', type=str, help='Address and port of the P2P network anchor node')
        parser.add_argument('-d', '--delay', type=int, default=0, help='Validation delay in milliseconds')

        args = parser.parse_args()

        port = args.port
        p2p_port = args.p2p_port
        anchor = args.anchor
        handicap = args.delay

        print(f"Starting node with the following configurations:")
        print(f"HTTP Port: {port}")
        print(f"P2P Port: {p2p_port}")
        if anchor:
            print(f"Anchor Address: {anchor}")
            # Convert anchor to tuple
            anchor_host, anchor_port = anchor.split(':')
            anchor = (anchor_host, int(anchor_port))
        print(f"Validation Handicap: {handicap}ms")

        # Initialize and start the DHT node
        dht_node = DHTNode(host='0.0.0.0', port=p2p_port, http_port=port, dht=anchor, handicap=handicap)
        dht_node.start()
    