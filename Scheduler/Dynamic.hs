module LwConc.Scheduler.Dynamic
( getNextThread
, schedule
, timeUp
, dumpQueueLengths
) where

-- This is a multi-level queue scheduler, taking priority into account.
--
-- It addresses the following problem:
-- -  If we pull from priority A's queue 15x more often than priority B's queue...
-- -  and there happen to be 15 threads at priority A, and only 1 at priority B...
-- => Then they all effectively run at the same priority.
--
-- This scheduler examines the length of each priority's queue, and pulls that many
-- before moving on to the next priority.
--
-- So while it may follow a simple order for priority, like
--    A AB ABC ABCD ABCDE
-- it's actually much more aggressive, because it'll run 15 threads from A, not just one.
--
-- Of course, the length of the queues may change; this is just a useful approximation.
-- It'll recalculate the numbers when it finishes the sequence of priorities.

import System.IO.Unsafe (unsafePerformIO)

import GHC.Arr as Array
import LwConc.PTM
import LwConc.Threads

timeUp :: IO Bool
timeUp = return True

type ReadyQ = PVar ([Thread])

-- |An array of ready queues, indexed by priority.
readyQs :: Array Priority ReadyQ
readyQs = listArray (minBound, maxBound) (unsafePerformIO $ sequence [newPVarIO [] | p <- [minBound .. maxBound :: Priority]])

queueLengths :: PVar (Array Priority Int)
queueLengths = unsafePerformIO $ newPVarIO undefined

priorityBox :: PVar [(Int, Priority)]
priorityBox = unsafePerformIO $ newPVarIO $ []

-- |Returns which priority to pull the next thread from, and updates the countdown for next time.
getNextPriority :: PTM Priority
getNextPriority =
  do l <- readPVar priorityBox
     case l of
       [] -> do est <- mapM (\prio -> do q <- readPVar (readyQs ! prio)
                                         return (length q, prio)) order
                writePVar priorityBox (Prelude.filter (\(x,_) -> x /= 0) est) -- skip empty priorities
                return A
       ((1,p):ps) -> do writePVar priorityBox ps
                        return p
       ((n,p):ps) -> do writePVar priorityBox ((n-1,p):ps)
                        return p
  where order = [A,B,A,B,C,A,B,C,D,A,B,C,D,E]

-- |Returns the next ready thread, or Nothing.
getNextThread :: PTM (Maybe Thread)
getNextThread = do priority <- getNextPriority
                   tryAt priority
  where tryAt priority = do let readyQ = readyQs ! priority
                            q <- readPVar readyQ
                            case q of
                              (t : ts) -> do writePVar readyQ ts
                                             return (Just t)
                              []       -> if priority == minBound
                                           then return Nothing
                                           else tryAt (pred priority) -- nothing to run at this priority, try something lower.

-- |Marks a thread "ready" and schedules it for some future time.
schedule :: Thread -> PTM ()
schedule thread@(Thread tcb _) =
  do priority <- getPriority tcb
     let readyQ = readyQs ! priority
     q <- readPVar readyQ
     writePVar readyQ (q ++ [thread])

dumpQueueLengths :: (String -> IO ()) -> IO ()
dumpQueueLengths cPrint = mapM_ dumpQL [minBound .. maxBound]
  where dumpQL :: Priority -> IO ()
        dumpQL p = do len <- atomically $ do q <- readPVar (readyQs ! p)
                                             return (length q)
                      cPrint ("|readyQ[" ++ show p ++ "]| = " ++ show len ++ "\n")
