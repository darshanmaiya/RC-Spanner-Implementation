#!/bin/bash

mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.twophasecommit.TwoPhaseCommit" > server_outputs/TwoPhaseCommitOutput.txt &
mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.twophasecommit.Client" > server_outputs/TwoPhaseCommitClientOutput.txt &
mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.paxos.ServerInitiator"  > server_outputs/PaxosServerOutput.txt &
