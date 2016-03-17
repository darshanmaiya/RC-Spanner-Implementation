#!/bin/bash

mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.twophasecommit.TwoPhaseCommit" > TwoPhaseCommitOutput.txt &
mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.twophasecommit.Client" > TwoPhaseCommitCLientOutput.txt &
mvn exec:java -Dexec.mainClass="edu.ucsb.cs274.paxos.Server1Initiator"  > PaxosServerOutput.txt &
