namespace Paxos

open BasicPaxos

module Operations =
  let newName (name:string) (Operation (_,f):Operation) : Operation =
    Operation (name,f)

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
    Operation (fname, op) //same req, we leave it untouched

  let write (v:Value) : string * (Value -> Value) = 
    ("write", fun _ -> v)

  let read : string * (Value -> Value) =
    ("read", id)

  let increment : string * (Value -> Value) = 
    let f vo = 
      match vo with
      | None -> Some "1"
      | Some v -> 
        let i = System.Int32.Parse v
        in Some ((i + 1).ToString())
    in ("increment",f)

  let readWithDefault d : string * (Value -> Value) =
    let f vo = 
      match vo with
      | None -> d
      | Some v -> Some v
    ("read", f)

  //The idea is
  //Represent leaderStatus as (leader,lease)
  //Each participant who is not the leader, will decrement lease by 1
  //The leader will renew its lease to `lease`. I.e. `lease` will function as the timeout.
  //
  //Example:
  //  3 participants
  //  leaderElection is applied every 1s by each participant
  //  `lease` is 4
  //  
  //  Then: Initially, the value will be None, and the first come will be the leader, and set its lease to `lease`
  //  All other participants will decrease his lease by 1, => lease is now 2
  //  If the leader goes down, he will not be able to renew his lease. And so the other two participants will decrease his lease.
  //  The participant who sets the current lease to 0, will obtain leadership, and initialize his lease to `lease`
  //  
  //  Whenever the current leader fails to renew his lease, he has to shutdown.
  //  The effective timeout (potential downtime) is 2 rounds
  let leaderElection (participant:string) (lease:int) : string * (Value -> Value) =
    let f (vo:Value) : string = 
      match vo with
      | None -> 
        participant + "," + (lease.ToString())
      | Some v -> 
        let s = v.Split(',');
        let leader = s.[0]
        let coins = System.Int32.Parse(s.[1])
        if (leader = participant)
        then leader + "," + (lease.ToString()) //preserve leader as me
        else if (coins = 0)
             then participant + "," + (lease.ToString()) //choose new leader
             else leader + "," + ((coins-1).ToString())//preserve other leader
    in ("leaderElection",f >> Some)
