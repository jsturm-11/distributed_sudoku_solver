import socket
import threading
import pickle
import time
import selectors
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from utils import find_next_empty, is_valid, split_array_in_middle
import argparse
import json
from sudoku import Sudoku
import queue
import uuid

class DHTNode(threading.Thread):
    def __init__(self, host, port, http_port, dht=None, handicap=1):
        '''Initialize node'''
        super().__init__()
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
        self.network = []
        self.task_queue = queue.Queue()
        self.validations = 0
        self.solved_count = 0 
        self.handicap = handicap / 1000
        self.solution = None
        self.solution_uuid = None
        self.http_server = threading.Thread(target=self.run_http_server)
        self.http_server.start()
        self.heartbeat_interval = 5
        self.last_heartbeat_time = time.time()
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.neighbor_tasks = queue.Queue()
        self.tupleStats= []
        self.coordinator = None


    def send_heartbeat(self):
        '''Sending heartbeat in the defined interval'''
        while self.running:
            if self.predecessor and (self.predecessor != self.host_port_tuple()):
                self.send_data({'method': 'HEARTBEAT'}, self.predecessor)
                self.send_data({'method': 'SOMETHING'}, self.host_port_tuple())
                print("I sent heartbeat to ", self.predecessor)
            else:
                # print("I did not send because:", self.predecessor)
                self.last_heartbeat_time = time.time()
            time.sleep(self.heartbeat_interval)
            

    def run_http_server(self):
        """Start the HTTP server."""
        server_address = (self.host, self.http_port)
        httpd = ThreadingHTTPServer(server_address, SudokuHandler)
        httpd.dht_node = self
        print(f'HTTP server started on {self.host, self.http_port}')
        httpd.serve_forever()


    def send_data(self, data, addr):
        """Send data to another node."""
        self.sock.sendto(pickle.dumps(data), addr)


    def non_blocking_receive(self):
        '''Receive data non blocking'''
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
                    'requestor': self.host_port_tuple()
                }
                self.send_data(join_request, self.dht)

            else:
                self.network.append(self.host_port_tuple())
                self.coordinator = self.host_port_tuple()
                self.inside_dht = True
                self.predecessor = self.host_port_tuple()
                self.neighbor = self.host_port_tuple()
        while self.running:
            events = self.selector.select(timeout=None)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
            
            self.check_neighbor()


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
        self.send_data({
                        'method': 'NODE_FAILED',
                        'node': self.host_port_tuple(),
            }, self.coordinator)
 
        self.running = False
        self.selector.unregister(self.sock)
        self.sock.close()
        self.selector.close()

    def check_neighbor(self):
        '''Check if neighbor is still alive'''
        if (self.neighbor != self.host_port_tuple()) and (time.time() - self.last_heartbeat_time > self.heartbeat_interval * 2):
            print("Neighbor died:", self.neighbor)
            self.handle_node_failure(self.neighbor)
            self.last_heartbeat_time = time.time()

    def handle_node_failure(self, failed_node):
        '''Handle a node failure'''
        if self.host_port_tuple() == self.coordinator:
            index_of_failed_node = self.network.index(failed_node)
            # Nodes to update
            before_failed_node = self.network[(index_of_failed_node - 1) % len(self.network)]
            after_failed_node = self.network[(index_of_failed_node + 1) % len(self.network)]
            # Update neighbor
            self.send_data({
                    'method': 'UPDATE_NEIGHBOR',
                    'neighbor': after_failed_node,
                }, before_failed_node)
            # Update predecessor of neighbor after
            self.send_data({
                    'method': 'UPDATE_PREDECESSOR',
                    'predecessor': before_failed_node,
                }, after_failed_node)
            # Update network
            self.network.remove(failed_node)
            for node in self.network:
                if node != self.host_port_tuple():
                    self.send_data({
                        'method': 'UPDATE_NETWORK',
                        'network': self.network,
                        'coordinator': self.coordinator
                    }, node)
        elif self.coordinator == failed_node:
            self.coordinator = self.host_port_tuple()
            self.handle_node_failure(failed_node)

        else:
            self.send_data({
                        'method': 'NODE_FAILED',
                        'node': failed_node,
            }, self.coordinator)

        while not self.neighbor_tasks.empty():
            self.task_queue.put(self.neighbor_tasks.get())

        if not self.task_queue.empty() and (self.task == []):
            self.task = self.task_queue.get()
            possible_solutions = self.task['range']
            sudoku = self.task['sudoku']
            uuid = self.task['uuid']
            self.perform_solving(sudoku, possible_solutions, uuid)
        

    def handle_request(self, sock, mask):
        '''Handle incoming request'''
        try:
            data, addr = self.receive_data()
            if data:
                self.handleMessage(data, addr)
        except Exception as e:
            print(f"Error receiving data: {e} on {self.port}")
    
    def handleMessage(self, data, addr):
        '''Handle an received message and run actions'''
        if data['method'] != 'TASK' or data['method'] != 'SOMETHING' :
            print(f"Received message: {data} from {addr}")
        if data['method'] == 'TASK':
            self.solution = None

            # If the node is already working, put the task in the queue
            if self.task != []:
                self.task_queue.put(data)
                return
            else:
                self.task_queue.put(data)

            # Work on all tasks in the queue
            while not self.task_queue.empty():
                next_task = self.task_queue.get()
                self.task = next_task
                possible_solutions = self.task['range']
                sudoku = self.task['sudoku']
                uuid = self.task['uuid']
                self.perform_solving(sudoku, possible_solutions, uuid)
            
            # After finishing the work, ask predecessor for work
            if self.predecessor != self.host_port_tuple():
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                # print(f"I ask for work after solving Node {self.port}")
            self.task = []
            
        elif data['method'] == 'NEEDWORK':
            # set neighbor to free if it asks for work
            self.neighborfree = True

        elif data['method'] == 'NODE_FAILED':
            failed_node = data['node']
            self.handle_node_failure(failed_node)

        elif data['method'] == 'JOIN_REQ':
            # Forward Join request to coordinator
            if self.host_port_tuple() != self.coordinator:
                self.send_data(data, self.coordinator)

            # Handle request if I am the coordinator
            else:
                incoming_request = data['requestor']
                # Update network
                self.network.append(incoming_request)
                for node in self.network:
                    if node != self.host_port_tuple():
                        self.send_data({
                            'method': 'UPDATE_NETWORK',
                            'network': self.network,
                            'coordinator': self.coordinator,
                        }, node)

                # Inform the first node about its new predecessor (the new node)
                self.send_data({
                    'method': 'UPDATE_PREDECESSOR',
                    'predecessor': incoming_request,
                }, self.network[0])

                # Inform the last node about its new neighbor (the new node)
                self.send_data({
                    'method': 'UPDATE_NEIGHBOR',
                    'neighbor': incoming_request,
                }, self.network[-2])

                # Inform the incoming request about its predecessor and successor
                self.send_data({
                    'method': 'JOIN_RES',
                    'predecessor': self.network[-2],
                    'neighbor': self.network[0],
                    'network': self.network,
                    'coordinator': self.coordinator
                }, incoming_request)


        elif data['method'] == 'JOIN_RES':
            new_predecessor = data['predecessor']
            new_neighbor = data['neighbor']
            new_network = data['network']
            new_coordinator = data['coordinator']

            # Update the predecessor to the sender of the JOIN_RES message
            self.predecessor = new_predecessor

            # Update the neighbor according to the information received
            self.neighbor = new_neighbor

            # Update the node network
            self.network = new_network

            # Update coordinator
            self.coordinator = new_coordinator

            # Update the dht status
            self.inside_dht = True

            # Ask for work if it does not have a task yet
            if self.predecessor != self.host_port_tuple() and self.task == []:
                # print(f"Node {self.port} asks for work")
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                # print(f"I ask for work because i got a join response {self.port}")

            # Neighbor is not free by default
            self.neighborfree = False

        elif data['method'] == 'UPDATE_PREDECESSOR':
            new_predecessor = data['predecessor']
            self.predecessor = new_predecessor

            # Ask for work if nothing to do
            if self.predecessor != self.host_port_tuple() and self.task == []:
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
        
        elif data['method'] == 'UPDATE_NEIGHBOR':
            new_neighbor = data['neighbor']
            self.neighbor = new_neighbor
            self.last_heartbeat_time = time.time()
            self.neighborfree = False

        elif data['method'] == 'SOLUTION_FOUND':

            print(f"Node {data['node']} found solution")
            print(f"With {self.validations} validations on {self.port}")
            self.solution = data['solution']
            self.solution_uuid = data['uuid']
            time.sleep(2)
            self.solved_count+= 1

            # Adapt queue and task of node
            temp_list = []
            while not self.task_queue.empty():
                item = self.task_queue.get()
                if item['uuid'] == self.solution_uuid:
                    print(f"Removing item: {item}")
                else:
                    temp_list.append(item)
                    print(f'Kept Item {item}')
            for item in temp_list:
                self.task_queue.put(item)

            temp_list = []
            while not self.neighbor_tasks.empty():
                item = self.neighbor_tasks.get()
                # When the uuid of the task in the queue is the same as the solution, delete from queue
                if item['uuid'] == self.solution_uuid:
                    print(f"Removing item: {item}")
                else:
                    temp_list.append(item)
                    print(f'Keeping item {item}')
            
            for item in temp_list:
                self.neighbor_tasks.put(item)

            if self.task and (self.task['uuid'] == self.solution_uuid):
                self.task = []
                if not self.task_queue.empty():
                    self.solution = None
                    self.solution_uuid = None
            # self.stop()

        elif data['method'] == 'UPDATE_NETWORK':
            self.network = data['network']
            self.coordinator = data['coordinator']

        elif data['method'] == 'HEARTBEAT':
            self.last_heartbeat_time = time.time()

        elif data['method'] == 'STOP':             
            self.stop()
        
        # send the requested data
        elif data['method'] == 'STATS_REQ':
            for node in self.network:
                  if node != self.host_port_tuple():
                    self.send_data({
                    'method': 'STATS_RES',
                    'validations': self.validations,
                    'address': self.host_port_tuple()
                }, node)
                    
        elif data['method'] == 'STATS_RES':
            validations = data['validations']
            address = data['address']
            print("\nPrinting tuple Stats:", address[0],address[1], validations) 
            if (address, validations) not in self.tupleStats:
                string = str(address[0]) + ":" + str(address[1]) 
                self.tupleStats.append((string, validations))
            print("\nPrinting tuple Stats:", self.tupleStats)



    def host_port_tuple(self):
        """Return the host and the port as a tuple"""
        return (self.host, self.port)

    def perform_solving(self, grid, arr, uuid):
        """Start solving a task and share with the other nodes if a solution was found"""
        solution = self.solve_sudoku(grid, uuid, arr)
        if solution == True:
            self.solved_count +=1
            self.solution= grid
            self.solution_uuid = uuid
            uuid = self.task['uuid']
            self.task = []
            temp_list = []
            while not self.task_queue.empty():
                item = self.task_queue.get()
                if item['uuid'] == self.solution_uuid:
                    print(f"Removing item: {item}")
                else:
                    temp_list.append(item)
                    print(f"Kept item: {item}")
            for item in temp_list:
                self.task_queue.put(item)

            # Adapt neighbor tasks in case of node failure
            temp_list = []
            while not self.neighbor_tasks.empty():
                item = self.neighbor_tasks.get()
                if item['uuid'] == self.solution_uuid:
                    print(f"Removing item: {item}")
                else:
                    temp_list.append(item)
            
            for item in temp_list:
                self.neighbor_tasks.put(item)
            
          
            print(f"Solution found on node {self.port}")
            
            for node in self.network:
                if node != self.host_port_tuple():
                    self.send_data({
                        'method': 'SOLUTION_FOUND',
                        'solution': grid,  # You might need to serialize the grid here
                        'node': self.host_port_tuple(),
                        'uuid': uuid
                    }, node)
            time.sleep(2)
            if not self.task_queue.empty():
                self.solution = None
                self.solution_uuid = None

    

    def solve_sudoku(self, puzzle, uuid, arr=range(1, 10)):
        '''Basic sudoku solving algotihm'''
        # solve sudoku using backtracking!
        # our puzzle is a list of lists, where each inner list is a row in our sudoku puzzle
        # return whether a solution exists
        # mutates puzzle to be the solution (if solution exists)
        
        if self.task == []:
            return False
        
        # Check for node updates
        data, addr = self.non_blocking_receive()
        if data:
            # Handle the message
            self.handleMessage(data, addr)

        # Check if neighbor is free and give him work
        if self.neighbor and self.neighborfree:
            # if neighbor free, send it task of the queue
            if not self.task_queue.empty():
                task = self.task_queue.get()
                self.send_data(task, self.neighbor)
                self.neighborfree = False
                self.neighbor_tasks.put(task)
            # if queue is empty, split current task into two halfs
            elif len(arr) >1:
                first_half, arr = split_array_in_middle(arr)
                # print(first_half, arr)
                task = {
                    'method': 'TASK',
                    'sudoku': puzzle,
                    'range': first_half,
                    'uuid': uuid,
                }
                self.send_data(task, self.neighbor)
                self.neighbor_tasks.put(task)
                self.neighborfree = False  

        # update validations
        self.validations +=1
        # step 1: choose somewhere on the puzzle to make a guess
        row, col = find_next_empty(puzzle)

        # step 1.1: if there's nowhere left, then we're done because we only allowed valid inputs
        if row is None:  # this is true if our find_next_empty function returns None, None
            return True 
        
        # step 2: if there is a place to put a number, then make a guess between 1 and 9
        for guess in arr: # range(1, 10) is 1, 2, 3, ... 9
            # Handicap for delaying the node
            time.sleep(self.handicap)
            # step 3: check if this is a valid guess
            if is_valid(puzzle, guess, row, col):
                # step 3.1: if this is a valid guess, then place it at that spot on the puzzle
                self.validations +=1
                puzzle[row][col] = guess
                # step 4: then we recursively call our solver!
                if self.solve_sudoku(puzzle, uuid=uuid):
                    return True
            
            # step 5: it not valid or if nothing gets returned true, then we need to backtrack and try a new number
            puzzle[row][col] = 0

        # step 6: if none of the numbers that we try work, then this puzzle is UNSOLVABLE!!
        return False

class SudokuHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        
            if self.path == '/solve':
                starttime = time.time()
                self.server.dht_node.solution = None
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                puzzle = json.loads(post_data)['sudoku']
                sudoku = Sudoku(puzzle)
                myuuid = uuid.uuid4()
                self.server.dht_node.send_data({'method': 'TASK','sudoku': sudoku.grid, 'range': range(1, 10), 'uuid': myuuid, 'initial_node': (self.server.dht_node.host,self.server.dht_node.port)}, (self.server.dht_node.host,self.server.dht_node.port ))
                
                while (self.server.dht_node.solution is None) or (self.server.dht_node.solution_uuid != myuuid):
                    time.sleep(0.01)
                solution = self.server.dht_node.solution
                elapsed_time = time.time() - starttime
                print("Also the server got the solution")

                
                self.send_response(201)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.server.dht_node.solution = None
                self.wfile.write(json.dumps({"solution": solution, "duration": elapsed_time}).encode('utf-8'))
            
    def do_GET(self):
        if self.path == '/stats':
            for node in self.server.dht_node.network:
                self.server.dht_node.send_data({'method': 'STATS_REQ',}, node)
            
            time.sleep(1)
            validation =  int(self.server.dht_node.validations)
            for i in range( len(self.server.dht_node.tupleStats)):
                validation += self.server.dht_node.tupleStats[i][1]
            stats = {
                "all": {
                    "solved": self.server.dht_node.solved_count,
                    "validations": validation
                },
                "nodes": [
                    {
                        "address": f"{self.server.dht_node.host}:{self.server.dht_node.port}",
                        "validations": self.server.dht_node.validations
                    }
                ]
            }
            if self.server.dht_node.tupleStats:
                for i in range( len(self.server.dht_node.tupleStats)):
                    new_node = {
                            "address":   self.server.dht_node.tupleStats[i][0],
                            "validation":   self.server.dht_node.tupleStats[i][1],
                        }
                    stats["nodes"].append(new_node)
            self.server.dht_node.tupleStats = []
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(stats).encode('utf-8'))

        elif self.path == '/network':
            network_info = {}
            for i in range(len(self.server.dht_node.network)):
                # Assuming each node object has 'address', 'predecessor', and 'neighbor' attributes
                node_address = self.server.dht_node.network[i]
                predecessor = self.server.dht_node.network[(i - 1) % len(self.server.dht_node.network)]
                neighbor = self.server.dht_node.network[(i + 1) % len(self.server.dht_node.network)]

                network_info[str(node_address)] = [str(predecessor), str(neighbor)]
            print(network_info)
            response = json.dumps(network_info, indent=4)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(response.encode())

def run(server_class=ThreadingHTTPServer, handler_class=SudokuHandler, port=8000):
        server_address = ('0.0.0.0', port)
        httpd = server_class(server_address, handler_class)
        print(f'Servidor HTTP iniciado na porta {port}')
        httpd.serve_forever()


if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Distributed Sudoku Solver Node")
        parser.add_argument('-p', '--port', type=int, required=True, help='HTTP port for the node')
        parser.add_argument('-s', '--p2p-port', type=int, required=True, help='P2P port for the node')
        parser.add_argument('-a', '--anchor', type=str, help='Address and port of the P2P network anchor node')
        parser.add_argument('-d', '--delay', type=int, default=1, help='Validation delay in milliseconds')

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


        def get_local_ip():
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                # Connecting to an external server (Google's DNS server)
                s.connect(('8.8.8.8', 80))
                local_ip = s.getsockname()[0]
            finally:
                s.close()
            return local_ip

        try: 
            dht_node = DHTNode(host=get_local_ip(), port=p2p_port, http_port=port, dht=anchor, handicap=handicap)
            dht_node.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Node stopping bc interrupt")
            #dht_node.stop()
    