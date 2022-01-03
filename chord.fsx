#if INTERACTIVE
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#endif

open System
open System.Collections.Generic
open Akka.FSharp
open Akka.Actor
open System.Threading

let system = ActorSystem.Create("ChordSystem")

type MessagesClient =
    | Init of int * int * IActorRef
    | CreateChord                    
    | JoinNode of IActorRef * int       
    | SetPredecessor of IActorRef * int
    | SendPredecessor of IActorRef
    | FindSuccessor of IActorRef * int
    | SetSuccessor of IActorRef * int
    | FindRequest of IActorRef * int * int 
    | GenerateFingers of int * int * IActorRef 
    | FixFingers    
    | SuccRefFuncIn  
    | SuccRefFuncFinal of int * IActorRef  
    | SuccRefFuncSch
    | HopsCounter of int
    | Notify of IActorRef * int    
    | NodeSch                  
    | RequestKey                

type MessagesMaster = 
    | InitNode of String       
    | CountMgr of int

let Client(mailbox: Actor<_>)=
    let p = 20
    let mutable n = 0 
    let mutable id = -1
    let mutable predecessorID = -1
    let mutable predecessorRef = null
    let mutable successorID = -1
    let mutable successorRef = null
    let mutable fingerTable = Array.create p 0
    let mutable fingerTableRef = Array.create p null
    let mutable reqs = 0
    let mutable masterRef = null
    let mutable sumOfHops = 0

    let rec loop()= actor{
       let! message = mailbox.Receive()
       
       match message with
        | Init (i,n,mRef) ->
            id <- i
            reqs <- n
            masterRef <- mRef

        | CreateChord ->
            predecessorID <- -1
            predecessorRef <- null
            successorID <- id
            successorRef <- mailbox.Self
            fingerTable.[0] <- id
            fingerTableRef.[0] <- mailbox.Self 

        | JoinNode (joinRef, joinID) ->
            if id <> successorID then
                mailbox.Self <! FindSuccessor(joinRef, joinID)
            else
                successorID <- joinID
                successorRef <- joinRef
                fingerTable.[0] <- joinID
                fingerTableRef.[0] <- joinRef
                joinRef <! SetSuccessor(mailbox.Self,id)
                joinRef <! SetPredecessor(null,-1)

        | SetPredecessor (predRef,predID) ->
            predecessorRef <- predRef
            predecessorID <- predID
        
        | SendPredecessor senderRef ->
            senderRef <! SuccRefFuncFinal(predecessorID,predecessorRef)

        | FindSuccessor (joinRef,joinID) ->
            if(id>successorID && joinID>id && joinID>=successorID) then
                joinRef <! SetSuccessor(successorRef,successorID)
            elif(id>successorID && joinID<id && joinID<=successorID) then
                joinRef <! SetSuccessor(successorRef,successorID)
            elif(joinID>id && joinID<=successorID) then
                joinRef <! SetSuccessor(successorRef,successorID)
            else
                successorRef <! FindSuccessor(joinRef,joinID)

        | SetSuccessor (succRef,succID) ->
            successorRef <- succRef
            successorID <- succID
            for i in [0..(p-1)] do
                fingerTable.[i] <- succID
                fingerTableRef.[i] <- succRef

        | FindRequest (joinRef,joinID,hopCount) ->
            if id = joinID then
                joinRef <! HopsCounter hopCount
            elif(id>successorID && joinID>id && joinID>=successorID) then
                joinRef <! HopsCounter hopCount
            elif(id>successorID && joinID<id && joinID<=successorID) then
                joinRef <! HopsCounter hopCount
            elif(joinID>id && joinID<=successorID) then
                joinRef <! HopsCounter hopCount
            else
                let mutable i = p-1
                while i>=0 do
                    let finger = fingerTable.[i]
                    let fingerRef = fingerTableRef.[i]
                    if (id>joinID && finger>id && finger>joinID) then
                        fingerRef <! FindRequest(joinRef,joinID, hopCount+1)                        
                        i <- -1
                    elif (id>joinID && finger<id && finger<joinID) then
                        fingerRef <! FindRequest(joinRef,joinID, hopCount+1)
                        i <- -1
                    elif (finger>id && finger<joinID) then
                        fingerRef <! FindRequest(joinRef,joinID, hopCount+1)
                        i <- -1
                    i<-i-1

        | GenerateFingers (i,joinID,joinRef) ->
            if (id=successorID) then
                fingerTable.[i] <- successorID
                fingerTableRef.[i] <- successorRef
            elif(id>successorID && joinID>id && joinID>=successorID) then
                fingerTable.[i] <- successorID
                fingerTableRef.[i] <- successorRef
            elif(id>successorID && joinID<id && joinID<=successorID) then
                fingerTable.[i] <- successorID
                fingerTableRef.[i] <- successorRef
            elif(joinID>id && joinID<=successorID) then
                fingerTable.[i] <- successorID
                fingerTableRef.[i] <- successorRef
            else
                successorRef <! GenerateFingers(i,joinID,joinRef)

        | FixFingers ->  
            n <- n+1
            if (n > (p-1)) then
                n <- 0
            let searchNodeId = (id + pown 2 n) % (pown 2 p)
            mailbox.Self <! GenerateFingers(n,searchNodeId,mailbox.Self)

        | SuccRefFuncIn ->      
            successorRef <! SendPredecessor(mailbox.Self)

        | SuccRefFuncFinal (nodeID, nodeRef) ->
            if nodeID <> -1 then
                if (id>successorID && nodeID>id && nodeID>successorID) then
                    successorID <- nodeID
                    successorRef <- nodeRef
                elif (id>successorID && nodeID<id && nodeID<successorID) then
                    successorID <- nodeID
                    successorRef <- nodeRef
                elif (nodeID>id && nodeID<successorID) then     
                    successorID <- nodeID
                    successorRef <- nodeRef
            successorRef <! Notify(mailbox.Self,id)
        
        | SuccRefFuncSch ->    
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromMilliseconds(1000.0), mailbox.Self, SuccRefFuncIn)    

        | HopsCounter hopCount->
            if reqs>0 then
                sumOfHops <- sumOfHops+hopCount
                reqs <- reqs-1

        | Notify (nodeRef,nodeID) ->
            if (predecessorID = -1 || (predecessorID>id && nodeID>predecessorID && nodeID>id)) then
                predecessorID <- nodeID
                predecessorRef <- nodeRef
            elif (predecessorID = -1 || (predecessorID>id && nodeID<predecessorID && nodeID<id)) then
                predecessorID <- nodeID
                predecessorRef <- nodeRef
            elif (predecessorID = -1 || (predecessorID<id && nodeID>predecessorID && nodeID<id)) then
                predecessorID <- nodeID
                predecessorRef <- nodeRef
            mailbox.Self <! FixFingers

        | NodeSch ->
            if reqs > 0 then
                mailbox.Self <! RequestKey
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1000.0), mailbox.Self, NodeSch)
            else
                masterRef <! CountMgr sumOfHops

        | RequestKey ->
            let rkey = Random().Next(1, pown 2 p)
            mailbox.Self <! FindRequest(mailbox.Self,rkey,1)

       return! loop()
    }
    loop()

let Master(mailbox: Actor<'a>) =
    let nodes = int (string (Environment.GetCommandLineArgs().[2]))
    let reqs = int(string (Environment.GetCommandLineArgs().[3]))
    let mutable cnt = 0
    let mutable sum = 0
    let mutable avg = 0.0
    let mutable addNode = null
    let nArr = Array.zeroCreate(nodes)
    let mutable setOfNodes = Set.empty
    let hopCounterArr = Array.zeroCreate(nodes)
    let p = 20
    let addressSpSize = pown 2 p
    let mutable temp = 0

    let rec loop()= actor{
        let! message = mailbox.Receive()
        match message with
        | InitNode _ ->
            while temp < nodes do
                let rnd = System.Random()
                let random = rnd.Next(1,addressSpSize)
                if setOfNodes.Contains(random) then
                    temp <- temp-1                
                else
                    setOfNodes <- setOfNodes.Add(random)
                    let actorSpace = string(random)
                    let chordClientRef = spawn system (actorSpace) Client
                    chordClientRef <! Init(random,reqs,mailbox.Self)
                    nArr.[temp] <- chordClientRef
                    if temp=0 then
                        printfn "%i %i"temp random
                        addNode <- chordClientRef
                        addNode <! CreateChord                       
                    else
                        addNode <! JoinNode(chordClientRef,random)
                        printfn "%i %i"temp random
                    
                    chordClientRef <! SuccRefFuncSch
                temp <- temp+1
            
            Thread.Sleep(45000)

            for i in [0..(nodes-1)] do
                Thread.Sleep(1000)

            for i in [0..(nodes-1)] do
                nArr.[i] <! NodeSch

        | CountMgr hopCount ->
            hopCounterArr.[cnt] <- hopCount
            cnt <- cnt+1
            if cnt = nodes then
                for i in hopCounterArr do
                    sum <- sum + i
                avg <- (float)sum/((float)nodes*(float)reqs)
                printfn "Average number of hops = %f" avg
                Environment.Exit(0)
        return! loop()
    }
    loop()

let masterRef = spawn system "ChordMaster" Master

masterRef <! InitNode ""

Console.ReadLine() |> ignore