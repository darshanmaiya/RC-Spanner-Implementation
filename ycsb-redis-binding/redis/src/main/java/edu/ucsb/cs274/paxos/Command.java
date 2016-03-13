package edu.ucsb.cs274.paxos;

public enum Command {
	COMMIT,
	READ,
	ABORT,
	ACCEPT,
	PREPARE,
	SUCCESS,		// To send read value from Client to Redis Client
	PROMISE,
	NACK,
	FAILURE
}
