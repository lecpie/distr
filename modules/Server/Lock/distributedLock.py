# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Rikard-Agravara algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictinaries: request, token,
        and peer_list) should be protected.
    --  the implementation should gratiously handle situations when a
        peer dies unexpectedly. All exceptions comming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated acordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.

"""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN

    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.

        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return dict(token)        

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """

        self.peer_list.lock.acquire()
        try:
            # if we are the lowest id peer on initialization then we should have the token
            # so that at least one peer in the system own the token.
            # We use the incremental id property, if we are the lowest id alive,
            # then we arrived first.

            if min(self.peer_list.peers) == self.owner.id:
                self.state = TOKEN_PRESENT

            pids = sorted(self.peer_list.peers.keys())

            self.token = {}
            for pid in pids:
                self.token[pid] = 0

        finally:
            self.peer_list.lock.release()

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """

        # if we we have token, maybe someone else asked for it
        if self.state == TOKEN_HELD:
            self.release()

        self.peer_list.lock.acquire()
        try:
            # if we hold the token then we should try to pass it to someone else
            
            # but only if there is someone to give it to
            if len (self.peer_list.peers) == 1:
                return

            # Iterate through the peer list circularly from our position
            # and try to give it to someone before leaving the system.
            pids = sorted(self.peer_list.peers.keys())
            cur = pids.index(self.owner.id)
            while self.state == TOKEN_PRESENT:
                # next index of pid in the list, circularly
                cur = cur + 1
                if cur == len(pids):
                    cur = 0

                pid = pids[cur]

                # try to give the token to the next peer in the list
                try:
                    self.peer_list.peers[pid].obtain_token(self._prepare(self.token))
                    self.state = NO_TOKEN
                except:
                    pass

        finally:
            self.peer_list.lock.release()

        pass

    def register_peer(self, pid):
        """Called when a new peer joins the system."""

        self.peer_list.lock.acquire()
        try:
            self.token[pid] = 0
        finally:
            self.peer_list.lock.release()

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""

        self.peer_list.lock.acquire()
        try:
            del self.token[pid]
            if pid in self.request:
                del self.request[pid]
        finally:
            self.peer_list.lock.release()

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")

        # if we don't have the  token, we have to request it
        if self.state == NO_TOKEN:
            
            self.peer_list.lock.acquire()
            try:
                # Go through all peers in the system other than us
                pids = sorted(self.peer_list.peers.keys())
                for pid in pids:
                    if pid == self.owner.id:
                        continue
                    try:
                        # Increment our clock before sending a message
                        self.time = self.time + 1


                        # The lock must be unlocked during the request or we will
                        # will need the lock here AND in obtain_token()
                        # as we wait for a reply but the token holder also wait for ours
                        peer = self.peer_list.peers[pid]
                        self.peer_list.lock.release()

                        # Send the request message with our clock and id
                        peer.request_token(self.time, self.owner.id)

                        self.peer_list.lock.acquire()
                    except:
                        pass
            finally:
                self.peer_list.lock.release()

        # wait for the token
        # Active waiting... bad, should be modified later
        while self.state == NO_TOKEN:
            pass

        # update our state : the token is now locked
        self.state = TOKEN_HELD

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        
        # If we don't have the token we shouldn't try to release it
        if self.state == NO_TOKEN:
            return

        self.peer_list.lock.acquire()
        try:

            # We do not lock the token anymore
            self.state = TOKEN_PRESENT

            # Go through all other peers id in a circular way
            pids = sorted(self.peer_list.peers.keys())
            cur = pids.index(self.owner.id)
            while True:
                # Take the next pid in the list
                cur = cur + 1

                # If we are off bound, go to the first pid in the ordered pid list
                if cur >= len(pids):
                    cur = 0

                # If the curcular iteration goes back to our pid, we keep the token
                if cur == pids.index(self.owner.id):
                    break

                pid = pids[cur]

                # If we should give the token to this peer.
                # The rule is that this token should have requested the token
                # and it should have requested it
                if pid in self.request and self.request[pid] > self.token[pid]:

                    # This pid is selected for copy, but maybe we can't reach it anymore
                    # So we copy the token status to be able to restore it
                    tokencpy = self.token.copy()

                    # Record this time as the last time we have the token
                    self.token[self.owner.id] = self.time

                    # Increment our clock before sending a message
                    self.time = self.time + 1

                    # Include the time this process got the token
                    self.token[pid] = self.time

                    try:
                        # send the token
                        self.peer_list.peers[pid].obtain_token(self._prepare(self.token))

                        # update our status, we no longer have the token
                        self.state = NO_TOKEN

                        break

                    except:
                        # Restore the token
                        token = tokencpy
                        
                        # Maybe we should also forget about this peer and unregister it ?
                        # But maybe it's going to happen as we locked the list
        finally:
            self.peer_list.lock.release()

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""

        self.peer_list.lock.acquire()
        try:
            # Update our logical clock
            self.time = max(time, self.time + 1)

            # Update request timestamp
            if pid in self.request:
                self.request[pid] = max(self.request[pid], self.time)
            else:
                self.request[pid] = self.time

        finally:
            self.peer_list.lock.release()

        # If we have the token but we don't need it then we release it
        if self.state == TOKEN_PRESENT:
            self.release()

        pass

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")

        self.peer_list.lock.acquire()
        try:        
            # Update our status and save the token
            self.token = self._unprepare(token)
            self.state = TOKEN_PRESENT

            # Update our logical clock
            self.time = max (self.time + 1, self.token[self.owner.id])

        finally:
            self.peer_list.lock.release()

        pass

    def display_status(self):
        """Print the status of this peer."""
        
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
