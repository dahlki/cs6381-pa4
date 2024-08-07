import threading
from kademlia.network import Server
import asyncio
import logging


class KademliaClient:

    def __init__(self, create, kademlia_port, kademlia_hosts, debug):
        self.logger = logging.getLogger('kademlia')
        self.debug = debug
        self.create = create
        self.kademlia_hosts = kademlia_hosts
        self.kademlia_port = kademlia_port
        self.kademlia_node = None

        # Create a lock for serializing get/set
        self.kad_lock = threading.Lock()

        # Initialize the event loop variable for the Kademlia server
        self.kad_loop = None

        # Get the current event loop
        loop = asyncio.get_event_loop()

        # Create a future to signal when Kademlia has been initialized
        self.start_future = loop.create_future()

        # Create a thread for running the kademlia event loop
        kthread = threading.Thread(group=None, target=self.kad_background_loop)

        # Start the kademlia thread
        kthread.start()

        # Wait for Kademlia to be initialized
        started = loop.run_until_complete(self.start_future)
        print(started, "!!!")

    # Performs a get against the Kademlia node, storing the result in the
    # resp_future parameter
    async def do_get(self, name, resp_future):
        await self.kademlia_node.bootstrap(self.kademlia_hosts)
        result = await self.kademlia_node.get(name)

        # Store the result in the future, using the future's own event loop
        resp_future.get_loop().call_soon_threadsafe(resp_future.set_result, result)

    # performs a set against the Kademlia node, storing True in the resp_future
    # parameter to indicate success
    async def do_set(self, name, value, resp_future):
        await self.kademlia_node.bootstrap(self.kademlia_hosts)
        await self.kademlia_node.set(name, value)

        # Store True in the resp_future to signal completion
        resp_future.get_loop().call_soon_threadsafe(resp_future.set_result, True)

    def kad_background_loop(self):
        print("in kad background loop!!!")
        try:
            # Create a new event loop
            self.kad_loop = asyncio.new_event_loop()

            # Make it the event loop for this thread
            asyncio.set_event_loop(self.kad_loop)

            # Initialized the Kademlia node
            self.kad_loop.run_until_complete(self.init_server())

            # Signal that Kademlia has been initialized
            self.start_future.get_loop().call_soon_threadsafe(self.start_future.set_result, True)

            # Run the kademlia event loop forever
            # This allows the Kademlia node to process requests in the background
            self.kad_loop.run_forever()
        except Exception as e:
            print(e, flush=True)

    # Performs a Kademlia get
    def get(self, name):
        print("in GET", name)

        # Lock to make sure there is only one pending get or set
        self.kad_lock.acquire()
        try:
            # Get the current event loop
            loop = asyncio.new_event_loop()

            # Create a future from the current event loop
            resp_future = loop.create_future()

            # Call do_get using the existing Kademlia event loop
            self.kad_loop.call_soon_threadsafe(asyncio.ensure_future, self.do_get(name, resp_future))

            # Wait for the result to be stored in resp_future and then return it
            return loop.run_until_complete(resp_future)
        finally:
            # Release the lock when finished
            self.kad_lock.release()

    def set(self, name, value):
        print("in SET", name, value)
        # Lock to make sure there is only one pending get or set
        self.kad_lock.acquire()
        try:

            # Get the current event loop
            loop = asyncio.new_event_loop()

            # Create a future from the current event loop
            resp_future = loop.create_future()

            # Call do_set using the existing Kademlia event loop
            self.kad_loop.call_soon_threadsafe(asyncio.ensure_future, self.do_set(name, value, resp_future))

            # Wait for a result to be stored in resp_future
            # indicating that the set is complete
            loop.run_until_complete(resp_future)
            return
        finally:
            # Release the lock when finished
            self.kad_lock.release()

    async def init_server(self):
        try:
            self.set_log_level(self.debug)
            # Create a Kademlia node
            self.kademlia_node = Server()

                # Set the port that the node listens on
            await self.kademlia_node.listen(self.kademlia_port)
            # if not self.create:
                # Provide a list of Kademlia hosts to bootstrap against
            await self.kademlia_node.bootstrap(self.kademlia_hosts)
        except Exception as e:
            print(e, flush=True)

    def set_log_level(self, debug):
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        if debug:
            self.logger.setLevel(logging.NOTSET)
        else:
            self.logger.setLevel(logging.WARNING)
