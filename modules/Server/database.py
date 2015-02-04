# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()

        # keep the db name
        self.db_file = db_file

        # array containing fortunes
        self.fortunes = []

        # read fortunes into the array 

        file = open(self.db_file, 'r')
        i = 0
        msg = ''
        for line in file:
            if line == '%\n':
                self.fortunes.insert(i, msg)
                msg = ''
                i = i + 1
            else:
                msg = msg + line

        file.close()

        pass

    def read(self):
        """Read a random location in the database."""

        if len(self.fortunes) == 0:
            print ('no message in the database')
            return

        # select the random index to take

        chosen = random.randint(0, len(self.fortunes) - 1)

        return self.fortunes[chosen]

        pass

    def write(self, fortune):
        """Write a new fortune to the database."""

        # write the new fortune into the db file
        with open(self.db_file, "a") as out:
            out.write(fortune)
            out.write('\n%\n')

        # add it to the internal fortune array
        self.fortunes.insert(len(self.fortunes), fortune)

        pass
