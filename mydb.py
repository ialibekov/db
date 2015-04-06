#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

__author__ = "Alibekov"


class MyDB(object):
    __NUM_OF_ARGS = dict(
        [
            ("GET", 1),
            ("SET", 2),
            ("BEGIN", 0),
            ("COMMIT", 0),
            ("ROLLBACK", 0),
            ("COUNTS", 1),
        ]
    )

    def __init__(self):
        self.db = dict()
        self.__transactions = list()
        print "Welcome to MyDB! Use the following commands (in uppercase only):" \
              "\n\t-GET\n\t-SET\n\t-BEGIN\n\t-COMMIT\n\t-ROLLBACK\n\t-COUNTS\nPress Ctrl+D to exit.\n"

    def handler(self):
        while True:
            line = sys.stdin.readline()
            if line == "":
                print "Exit"
                break
            self.__executer(line)
            print ""

    def __print_to_console(self, s=""):
        print "\n\t{}".format(s)

    def __get(self, key):
        value = "NULL"
        if self.__transactions:
            for transaction in reversed(self.__transactions):
                value = transaction.get(key, "NULL")
                if value != "NULL":
                    break
        if value == "NULL":
            value = self.db.get(key, "NULL")
        self.__print_to_console(value)
        return value

    def __set(self, key, value):
        if self.__transactions:
            self.__transactions[-1][key] = value
        else:
            self.db[key] = value

    def __begin(self):
        self.__transactions.append(dict())

    def __commit(self):
        if self.__transactions:
            for transaction in self.__transactions:
                self.db.update(transaction)
            self.__transactions = list()
        else:
            self.__print_to_console("ERROR: Nothing to commit!")

    def __rollback(self):
        if self.__transactions:
            del self.__transactions[-1]
        else:
            self.__print_to_console("ERROR: Nothing to rollback!")

    def __counts(self, value):
        count = 0
        watched_keys = set()
        if self.__transactions:
            for transaction in reversed(self.__transactions):
                for k, v in filter(lambda (x, y): y == value, transaction.iteritems()):
                    if k not in watched_keys:
                        count += 1
                watched_keys.update(transaction.keys())
            for k, v in filter(lambda (x, y): y == value, self.db.iteritems()):
                if k not in watched_keys:
                    count += 1
        else:
            count = self.db.values().count(value)
        self.__print_to_console(count)
        return count

    def __executer(self, line):
        command_with_args = line.split()
        if command_with_args:
            command, args = command_with_args[0], command_with_args[1:]
            num_of_args = len(args)
            if command == "GET" and num_of_args == self.__NUM_OF_ARGS["GET"]:
                    return self.__get(args[0])
            elif command == "SET" and num_of_args == self.__NUM_OF_ARGS["SET"]:
                    return self.__set(args[0], args[1])
            elif command == "BEGIN" and num_of_args == self.__NUM_OF_ARGS["BEGIN"]:
                    return self.__begin()
            elif command == "COMMIT" and num_of_args == self.__NUM_OF_ARGS["COMMIT"]:
                    return self.__commit()
            elif command == "ROLLBACK" and num_of_args == self.__NUM_OF_ARGS["ROLLBACK"]:
                    return self.__rollback()
            elif command == "COUNTS" and num_of_args == self.__NUM_OF_ARGS["COUNTS"]:
                    return self.__counts(args[0])
        self.__print_to_console("ERROR: Invalid command!")


def main():
    db = MyDB()
    db.handler()


if __name__ == "__main__":
    main()
