open System
open System.Threading
open System.Configuration
open Paxos.LeaderElection
open Paxos.LeaderElection.LeaderElection

//Config
let endpoints = ConfigurationManager.AppSettings.["endpoints"].Split(';')
let config = 
  {
    Me = ConfigurationManager.AppSettings.["me"]
    MeEndpoint = ConfigurationManager.AppSettings.["meEndpoint"]
    Endpoint1 = endpoints.[0]
    Endpoint2 = endpoints.[1]
    LeadershipTimeout = TimeSpan.FromSeconds 2.0
  }

let leaderUpdated lo =
  match lo with
  | None -> printfn "No leader"
  | Some l -> printfn "Leader is %s" l

[<EntryPoint>]
let main argv = 
  //Console
  let cancel = new CancellationTokenSource()
  System.Console.CancelKeyPress.Add(fun _ -> cancel.Cancel())
  Console.Title <- config.Me

  //Loop
  LeaderElection.loop config cancel.Token leaderUpdated
  0
