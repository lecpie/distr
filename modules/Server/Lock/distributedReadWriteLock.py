# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 30 January 2015
#
# Copyright 2012-2015 Linkoping University
# -----------------------------------------------------------------------------

"""Class implementing a distributed version of ReadWriteLock."""

import threading
from . import readWriteLock


class DistributedReadWriteLock(readWriteLock.ReadWriteLock):

    """Distributed version of ReadWriteLock."""

    def __init__(self, distributed_lock):
        readWriteLock.ReadWriteLock.__init__(self)
        # Create a distributed lock
        self.distributed_lock = distributed_lock
        
        #Create a lock protecting the distributed lock acquiring
        self.distributed_lock_access = threading.Lock()

        pass

    # Public methods

    def write_acquire(self):
        """Acquire the rights to write into the database.

        Override the write_acquire method to include obtaining access
        to the rest of the peers.

        """
        self.distributed_lock_access.acquire()
        self.distributed_lock.acquire()

        readWriteLock.ReadWriteLock.write_acquire(self)

        pass

    def write_release(self):
        """Release the rights to write into the database.

        Override the write_release method to include releasing access
        to the rest of the peers.

        """        
        self.distributed_lock.release()
        self.distributed_lock_access.release()

        readWriteLock.ReadWriteLock.write_release(self)
        
        pass

    def write_acquire_local(self):
        readWriteLock.ReadWriteLock.write_acquire(self)

    def write_release_local(self):
        readWriteLock.ReadWriteLock.write_release(self)
