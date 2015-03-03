# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Package for handling a list of objects of the same type as a given one."""

import threading
from Common import orb


class PeerList(object):

    """Class that builds a list of objects of the same type as this one."""

    def __init__(self, owner):
        self.owner = owner
        self.lock = threading.Condition()
        self.peers = {}

    # Public methods

    def initialize(self):
        """Populates the list of existing peers and registers the current
        peer at each of the discovered peers.

        It only adds the peers with lower ids than this one or else
        deadlocks may occur. This method must be called after the owner
        object has been registered with the name service.

        """

        self.lock.acquire()
        try:
            # Ask for the peer list from the name service.
            print ("requesting peer list")
            rep = self.owner.name_service.require_all(self.owner.type)

            # Create a stub object for each peer listed by the nameservice.
            for pid, paddr in rep:
                try:
                    # Create the stub object.
                    stub = orb.Stub(paddr)

                    # Only create a stub object for this peer if it's not us.
                    # And if its pid is lower, if it is higher it probably means
                    # that we will receive a registration message and add it two times
                    # This probably needs rethinking to avoid relying on incremental ids.
                    if pid < self.owner.id:
                        # Send the registration message.
                        stub.register_peer(self.owner.id, self.owner.address)
                    
                    # Only add it to our list if we could be registered by this peer.
                    # or if it's us
                    self.peers[pid] = stub

                except:
                    # One peer failing to be registered should not crash the whole program.
                    # So we just do without him if we can't reach him.
                    pass

        finally:
            self.lock.release()

    def destroy(self):
        """Unregister this peer from all others in the list."""

        self.lock.acquire()
        try:
            #
            # Your code here.
            #

            pids = sorted(self.peers.keys())
            for pid in pids:
                try:
                    if pid != self.owner.id:
                        self.peers[pid].unregister_peer(self.owner.id)
                except:
                    pass

            pass
        finally:
            self.lock.release()

    def register_peer(self, pid, paddr):
        """Register a new peer joining the network."""

        # Synchronize access to the peer list as several peers might call
        # this method in parallel.
        self.lock.acquire()
        try:
            self.peers[pid] = orb.Stub(paddr)
            print("Peer {} has joined the system.".format(pid))
        finally:
            self.lock.release()

    def unregister_peer(self, pid):
        """Unregister a peer leaving the network."""
        # Synchronize access to the peer list as several peers might call
        # this method in parallel.

        self.lock.acquire()
        try:
            if pid in self.peers:
                del self.peers[pid]
                print("Peer {} has left the system.".format(pid))
            else:
                raise Exception("No peer with id: '{}'".format(pid))
        finally:
            self.lock.release()

    def display_peers(self):
        """Display all the peers in the list."""

        self.lock.acquire()
        try:
            pids = sorted(self.peers.keys())
            print("List of peers of type '{}':".format(self.owner.type))
            for pid in pids:
                addr = self.peers[pid].address
                print("    id: {:>2}, address: {}".format(pid, addr))
        finally:
            self.lock.release()

    def peer(self, pid):
        """Return the object with the given id."""

        self.lock.acquire()
        try:
            return self.peers[pid]
        finally:
            self.lock.release()

    def get_peers(self):
        """Return all registered objects."""

        self.lock.acquire()
        try:
            return self.peers
        finally:
            self.lock.release()
