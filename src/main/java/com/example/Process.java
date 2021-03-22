package com.example;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;

import java.time.Duration;
import java.util.ArrayList;


public class Process extends UntypedAbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);// Logger attached to actor
    private final int N;//number of processes
    private final int id;//id of current process
    private Members processes;//other processes' references
    private Integer proposal;
    private int ballot;
    private int readBallot;
    private int imposeBallot;
    private Integer estimate;
    private ArrayList<Pair<Integer,Integer>> states;
    private long startTime = 0;

    private boolean proposingStatus = true;
    private boolean faultProne = false;
    private boolean silent = false;
    private double crashProbability = 0.2; //to configure
    private int timeToProposeAgain = 50; //to configure, in ms
    private boolean hold = false;
    private boolean decided = false;

    private int oldAbortBallot = 0;
    private int oldGatherBallot = 0;
    private int ackCount = 0;
    private int stateEntryZero = 0;

    private boolean verbose = true; //to configure


    public Process(int ID, int nb) {
        N = nb;
        id = ID;
        ballot = id - N;
        readBallot = 0;
        imposeBallot = id - N;
        estimate = null;
        proposal = null;
        resetStateArrayList(N);

        oldAbortBallot = -2*N;
        oldGatherBallot = -2*N;

        getContext().system().scheduler().scheduleOnce(Duration.ofMillis(timeToProposeAgain), getSelf(), "propose_again", getContext().system().dispatcher(), ActorRef.noSender());

    }
    
    public String toString() {
        return "Process{" + "id=" + id ;
    }

    /**
     * Static function creating actor
     */
    public static Props createActor(int ID, int nb) {
        return Props.create(Process.class, () -> {
            return new Process(ID, nb);
        });
    }
    
    
    private void ofconsProposeReceived(Integer v) {
        proposingStatus = true;
        proposal = v;
        ballot += N;
        resetStateArrayList(N);
        if (verbose) {log.info("p"+self().path().name()+" proposed        (proposal="+ proposal + ")");}
        for (ActorRef actor : processes.references) {
            actor.tell(new ReadMsg(ballot), this.getSelf());
        }
    }

    private void launchReceived() {
        Integer v = 0;
        if (Math.random()>0.5){
            v = 1;
        }
        ofconsProposeReceived(v);
    }
    
    private void readReceived(int newBallot, ActorRef pj) {

        if (readBallot>newBallot || imposeBallot>newBallot){
            //log.info("r: "+getSender().path().name() + " sends ABORT to " + pj.path().name());
            pj.tell(new AbortMsg(newBallot), getSelf());
        } else {
            readBallot = newBallot;
            pj.tell(new GatherMsg(newBallot, imposeBallot, estimate), this.getSelf());
        }
    }

    private void gatherReceived(int newBallot, int estBallot, Integer estimate, ActorRef pj){
        states.set(Integer.valueOf(pj.path().name()), new Pair<Integer,Integer>(estimate, estBallot));
        if(estBallot==0) {stateEntryZero=1;} //case where estBallot==0 so "getNumberOfEntries" does not count it as an entry
        int numberOfStateEntries = getNumberOfEntries(states)+stateEntryZero;
         

        log.info("p"+self().path().name() +" entries: "+numberOfStateEntries+" | " + states.toString());
        if (numberOfStateEntries>(N/2) && oldGatherBallot!=newBallot){ //majority
            //log.info(newBallot+"");
            oldGatherBallot = newBallot;

            int maxBallot = 0;
            for (int i = 0; i < states.size(); i++) {
                if (states.get(i).second() > maxBallot) {
                    maxBallot = states.get(i).second();
                    proposal = states.get(i).first();
                }
            }
            resetStateArrayList(N);

            for (ActorRef actor : processes.references) {
                actor.tell(new ImposeMsg(ballot, proposal), this.getSelf());
                //log.info("Impose (proposal: " + proposal + ") ballot " + ballot + " msg: p" + self().path().name() + " -> p" + actor.path().name());
             }
        }
    }

    private void imposeReceived(int newBallot, Integer v, ActorRef pj){
        if (readBallot > newBallot || imposeBallot > newBallot) {
            log.info("p"+self().path().name() +": "+readBallot+" > "+newBallot+" || "+imposeBallot +">"+ newBallot);
/*             if (readBallot > newBallot){
                log.info("read>new");
            }
            if (imposeBallot > newBallot){
                log.info("impose>new");
            } */
            //log.info("i: "+getSender().path().name() + " sends ABORT to " + pj.path().name());
            pj.tell(new AbortMsg(newBallot), this.getSender());
        } else {
            estimate = v;
            imposeBallot = newBallot;
            pj.tell(new AckMsg(newBallot), this.getSender());
        }
    }

    private void ackReceived(int ballot){
        this.ackCount++;
        //log.info("COUNT0 "+this.ackCount);
        if (this.ackCount>(N/2)){
            this.ackCount = 0;
            //log.info("COUNT1 "+this.ackCount);
            if (verbose){
                log.info("p"+self().path().name()+" received ACK    from ALL" + " (b="+ ballot + ")");
            }
            for (ActorRef actor : processes.references) {
                actor.tell(new DecideMsg(proposal), this.getSelf());
                //log.info("Decide ballot " + ballot + " msg: p" + self().path().name() + " -> p" + actor.path().name());
            }
        }
    }

    private void decideReceived(Integer v){
        if (!decided){
            decided = true;
            if (true){
                long elapsedTime =  System.currentTimeMillis() - startTime;
                log.info("p"+self().path().name()+" received DECIDE from p" + getSender().path().name() + " (value="+ v + ")"+ " | time: " + elapsedTime);
            }
            for (ActorRef actor : processes.references) {
                actor.tell(new DecideMsg(proposal), this.getSelf());
            }
             silent = true;
        }
    } 

    private void abortReceived(int ballot) {
        
        if(ballot != oldAbortBallot && !decided){
            oldAbortBallot = ballot;
            proposingStatus = false;
            if(!hold){
                /* getContext().system().scheduler().scheduleOnce(Duration.ofMillis(timeToProposeAgain), getSelf(), "propose_again", getContext().system().dispatcher(), ActorRef.noSender());
                //ofconsProposeReceived(proposal);
                log.info(self().path().name() +" has hold="+hold); */
            }
            
        }
    }
    
    
    public void onReceive(Object message) throws Throwable {
        if (faultProne && !silent) {
            double draw = Math.random();
            if (draw<crashProbability) {
                silent = true;
                if (verbose){log.info("p" + self().path().name() + " has crashed - enters silent mode");}
            }
        }
        if (!silent) {
            if (message instanceof Members) {//save the system's info
                Members m = (Members) message;
                processes = m;
                if (verbose){log.info("p" + self().path().name() + " received processes info");}
            }
            else if (message instanceof OfconsProposerMsg) {
                OfconsProposerMsg m = (OfconsProposerMsg) message;
                if (verbose){log.info("p" + self().path().name() + " received OfconsProposerMsg: " + m.v);}
                this.ofconsProposeReceived(m.v);
            }
            else if (message instanceof ReadMsg) {
                ReadMsg m = (ReadMsg) message;
                //log.info("Read ballot " + ballot + " msg: p" + self().path().name() + " -> p" + actor.path().name());
                if (verbose){log.info("p"+self().path().name()+" received READ   from p" + getSender().path().name() + " (b="+ m.ballot + ")");}
                this.readReceived(m.ballot, getSender());
            }
            else if (message instanceof AbortMsg) {
                AbortMsg m = (AbortMsg) message;
                if (verbose){log.info("p"+self().path().name()+" received ABORT  from p" + getSender().path().name() + " (b="+ m.ballot + ")");}
                this.abortReceived(m.ballot);
            }
            else if (message instanceof GatherMsg) {
                GatherMsg m = (GatherMsg) message;
                if (verbose){log.info("p"+self().path().name()+" received GATHER from p" + getSender().path().name() + " (b="+ m.ballot + ")");}
                this.gatherReceived(m.ballot, m.imposeBallot, m.estimate, getSender());
            }
            else if (message instanceof ImposeMsg) {
                ImposeMsg m = (ImposeMsg) message;
                if (verbose) {log.info("p"+self().path().name()+" received IMPOSE from p" + getSender().path().name() + " (b="+ m.ballot + ", prop="+m.proposal+")");}
                this.imposeReceived(m.ballot, m.proposal, getSender());
            }
            else if (message instanceof AckMsg) {
                AckMsg m = (AckMsg) message;
                //log.info("p"+self().path().name()+" received ACK    from p" + getSender().path().name() + " (b="+ m.ballot + ")");
                this.ackReceived(m.ballot);
            }
            else if (message instanceof DecideMsg) {
                DecideMsg m = (DecideMsg) message;
                //log.info("p"+self().path().name()+" received DECIDE from p" + getSender().path().name() + " (value="+ m.v + ")");
                this.decideReceived(m.v);
            }
            else if (message instanceof CrashMsg) {
                if (verbose){log.info("p"+self().path().name()+" received CRASH  from p" + getSender().path().name() + " - enters fault-prone mode");}
                faultProne = true;
            }
            else if (message instanceof LaunchMsg) {
                if (verbose){log.info("p"+self().path().name()+" received LAUNCH from p" + getSender().path().name());}
                launchReceived();
            }
            else if (message instanceof HoldMsg) {
                if (verbose){log.info("p"+self().path().name()+" received HOLD   from p" + getSender().path().name());}
                hold = true;
            }
            else if(message instanceof StartTime){
                startTime = ((StartTime)message).time;
            }
            else if(message instanceof String){
                if ((String)message=="propose_again"){
                    if (!hold && !proposingStatus) {ofconsProposeReceived(proposal);}
                    getContext().system().scheduler().scheduleOnce(Duration.ofMillis(timeToProposeAgain), getSelf(), "propose_again", getContext().system().dispatcher(), ActorRef.noSender());
                }
            }
        }
        
    }

    private void resetStateArrayList(int length){
        stateEntryZero = 0;
        states = new ArrayList<Pair<Integer,Integer>>();
        for (int i = 0; i < length; i++) {
            states.add(new Pair<Integer,Integer>(null, 0));
          }
    }

    private int getNumberOfEntries(ArrayList<Pair<Integer,Integer>> array){
        int count = 0;
        for(Pair<Integer,Integer> pair : array){
            if (pair.second()!=0){
                count++;
            }
        }
        return count;
    }


}
