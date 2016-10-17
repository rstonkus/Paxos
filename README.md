## Paxos
This is an implementation of the basic Paxos algoritm originally described by Leslie Lamport in
[Time, Clocks and the Ordering of Events in a Distributed System](http://research.microsoft.com/en-us/um/people/lamport/pubs/pubs.html#time-clocks). See also [Wikipedia](https://en.wikipedia.org/wiki/Paxos_(computer_science)), for a more accessible explanation.

## Functionality

It is implemented using the following messages and roles directly

```
Client   Proposer      Acceptor     Learner
   |         |          |  |  |       |  |
   X-------->|          |  |  |       |  |  Request
   |         X--------->|->|->|       |  |  Prepare(1)
   |         |<---------X--X--X       |  |  Promise(1,{Va,Vb,Vc})
   |         X--------->|->|->|       |  |  Accept!(1,Vn)
   |         |<---------X--X--X------>|->|  Accepted(1,Vn)
   |<---------------------------------X--X  Response
   |         |          |  |  |       |  |
```

An operation is a named function
```fsharp
  type Value = string option
  type ValueLastTouched = Value * (Sender * Guid) list
  type Operation = Operation of string * (ValueLastTouched option -> ValueLastTouched)
```

Paxos does not guarantee idempotency. The list attached to each value: ```(Sender * Guid) list``` is useful for implementing it on top. 
It is a list of (proposer, request-guid), and will not grow longer than the amount of proposers.
Idempotency is implemented using
```fsharp
  let idempotent ((fname,f):string * (Value -> Value)) (sender:Sender) (g:System.Guid) : Operation =
    let op vo = 
      match vo with
      | None -> (f None,[(sender,g)])
      | Some (v,xs) -> 
        let xo = List.tryFind (fun (_,guid) -> guid = g) xs
        match xo with
        | None -> let xs' = List.filter (fst >> (<>) sender) xs
                  in (f v, (sender,g)::xs')
        | Some _ -> (v,xs)
    Operation (fname, op)
```

Even though people claim that paxos does not get stuck, I fail to see how it is possible (messages can be lost).
So, it is crutial to support retries. The core algorithm defines time as
```fsharp
  type Time = int64
```
And it is the user of the library that defines the timeout
```fsharp
  hastimedout:Time -> bool
```
If the timeout is set too low, it might cause an infinite loop of retries! 

## Test

The core algorithm implemented in BasicPaxos.fs has been thouroughly tested by emulating a distributed system.
The emulator runs in a single thread and implements random interleavings, processor crashes and message losses.

The following pseudo fsharp shows the essence of the emulator
```fsharp
  let runToEnd (debugIn:bool) (random:System.Random) emulateMessageLosses quorumSize participants result = 
    while noMoreMessages
      do
        match random.Next 5 with
        |0|1|2|3 -> //progress
          match random.Next 6 with
          | 0 | 1 | 2 -> ``consume 1 message by 1 randomly chosen participant``
          | 3 | 4 -> ``send``
          | _ -> ``checkTimeout``
        | _ ->
          if (random.Next 50 <> 0)  // P(evil) = 1/5 * 1/20 = 1/100
          then () 
          else if (``possible to crash without breaking the quorum``) //TODO This should not be necessary
		       then ``crash random participant for x rounds + clear input messages``
```

Since the processes are randomly interleaved, a crash might not manifest as "not receiving new messages", 
and as such it also models pure message loss (albeit with a somewhat low probability).

The test runs 2*2000 requests for incrementing a common value by two clients targeting each their proposer.

I have not yet been able to produce an inconsistence in the test.


## Performance
I have not tested the performance, but I do expect it to be poor. It has the problem of dualing proposers. 
If one wanted to use it for high-throughput, once must either implement Multi-Paxos or at least find a way 
of directing the requests to one proposer at a time.


## TODOs (and known bugs)

 - The emulator does currently not duplicate messages, and in fact the algorithm does not yet handle this scenario. It could however easily be extended to do so.
 - The quorum check before crashing a process in the emulator should not be necessary

