namespace Paxos.LeaderElection

open System
open NetMQ.Sockets
open NetMQ
open Paxos
open Paxos.BasicPaxos
open Newtonsoft

module Serialize = 
  type DestinationDto() =
    member val destination : Destination = Unchecked.defaultof<Destination> with get,set
    member val msg : BasicPaxos.Msg = Unchecked.defaultof<BasicPaxos.Msg> with get,set
  let serialize (d:Destination) (m:BasicPaxos.Msg) =
    let dto = DestinationDto(destination = d, msg = m)
    let s = Json.JsonConvert.SerializeObject(dto)
    in s

  let deserialize (s:string) : Destination * BasicPaxos.Msg = 
    let dto = Json.JsonConvert.DeserializeObject<DestinationDto>(s)
    in (dto.destination,dto.msg)

module ZMQ =
  
  type ZMQMessage =
    {
      RoutingKey:string
      Sender:string
      Msg:string
    }
  
  type ZMQTransport(me:string, meEndpoint:string, otherEndpoint1:string, otherEndpoint2:string) =
    let subscribe e =
      let uri = Uri(e)
      
      //MUST lookup dns up front. Otherwise reconnect takes too long if no connection is availble
      let ip = if uri.IsLoopback
               then "127.0.0.1"
               else let ips = Net.Dns.GetHostEntry(uri.DnsSafeHost).AddressList 
                    ips 
                    |> Seq.find (fun e -> e.AddressFamily = Net.Sockets.AddressFamily.InterNetwork)
                    |> (fun i -> i.ToString())
      
      let conString = sprintf "tcp://%s:%i" ip uri.Port
      let o = new SubscriberSocket(conString)
      let () = o.Subscribe("$all")
      let () = o.Subscribe(me) 
      (o,conString)
    let reconnect (s:SubscriberSocket) (e:string) =
      //let start = DateTime.Now
      try s.Disconnect e with _ -> ()
      try s.Connect e with _ -> ()
      //printfn "%s %f" who (DateTime.Now-start).TotalMilliseconds
    let publisherSocket = new PublisherSocket(meEndpoint)
    let (subscriberSocket1,conString1) = subscribe otherEndpoint1
    let (subscriberSocket2,conString2) = subscribe otherEndpoint2
    let frames : System.Collections.Generic.List<string> ref = ref null
    let mutable lastReceived1 = DateTime.Now
    let mutable lastReceived2 = DateTime.Now
    member this.Send (d:Destination) (m:BasicPaxos.Msg) =
      let destToRoutingKey d =
        match d with
        | Proposer s -> s
        | Broadcast -> "$all"
        | BroadcastAcceptors -> "$all"
        | External _ -> failwith "should not be sent over the wire"
        | Acceptor s -> s
      let m =
        {
          RoutingKey=destToRoutingKey d
          Sender = me
          Msg = (Serialize.serialize d m)
        }
      publisherSocket.SendMoreFrame(m.RoutingKey)
                     .SendMoreFrame(m.Sender)
                     .SendFrame(m.Msg)
  
    member this.CheckConnections() = 
      if (DateTime.Now - lastReceived1 > TimeSpan.FromSeconds 5.0)
      then do reconnect subscriberSocket1 conString1 //TODO This takes too damn long if no cable is in the nic: 2,5s !!! (1ms if it is) --- Ah, it is the nslookup!!
              lastReceived1 <- DateTime.Now
      if (DateTime.Now - lastReceived2 > TimeSpan.FromSeconds 5.0)
      then do reconnect subscriberSocket2 conString2
              lastReceived2 <- DateTime.Now
  
    member this.Receive (handle:Sender * Destination * BasicPaxos.Msg -> unit) =
      let r (s:SubscriberSocket) =
        if s.TryReceiveMultipartStrings(frames,2)
        then let fs = frames.Value
             let zm = {RoutingKey = fs.[0]
                       Sender = fs.[1]
                       Msg = fs.[2]}
             let (d,m) = Serialize.deserialize zm.Msg
             in Some (zm.Sender,d,m)
        else None
      r subscriberSocket1 |> Option.iter (handle >> (fun () -> lastReceived1 <- DateTime.Now ))
      r subscriberSocket2 |> Option.iter (handle >> (fun () -> lastReceived2 <- DateTime.Now ))
  
  
    interface System.IDisposable with 
      member this.Dispose() =
        publisherSocket.Dispose()
        subscriberSocket1.Dispose()
        subscriberSocket2.Dispose()
        