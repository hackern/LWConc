{-# LANGUAGE ScopedTypeVariables #-}
module LwConc.Threads
( ThreadId(..)
, tidTLSKey
, mySafeThreadId
, newThreadId
, Priority(..)
, getPriority
, setPriority
, myPriority
, setMyPriority
, Thread(..)
, switchT
, blockSignalsKey
, checkSignals
, block
, unblock) where

import LwConc.Substrate
import LwConc.PTM
import GHC.Arr(Ix(..))
import Control.Exception (catch, AsyncException(..), SomeException(..))
import GHC.Exception(Exception, throw)
import System.IO.Unsafe(unsafePerformIO)


data Priority = E | D | C | B | A
  deriving (Show, Eq, Ord, Bounded, Enum, Ix)

getPriority :: ThreadId -> PTM Priority
getPriority (TCB _ _ pv) = readPVar pv

setPriority :: ThreadId -> Priority -> PTM ()
setPriority (TCB _ _ pv) p = writePVar pv p


myPriority :: PTM Priority
myPriority = do m <- mySafeThreadId
                case m of
                  Nothing  -> return maxBound -- uninitialized thread
                  Just tid -> getPriority tid

setMyPriority :: Priority -> PTM ()
setMyPriority p = do m <- mySafeThreadId
                     case m of
                       Nothing -> return ()
                       Just tid -> setPriority tid p

data ThreadId = TCB Int (PVar ([AsyncException])) (PVar Priority)

instance Eq ThreadId where
  (TCB _ xbox _) == (TCB _ ybox _) = xbox == ybox

instance Show ThreadId where
  show (TCB id _ _) = "<Thread " ++ show id ++ ">"


tidTLSKey :: TLSKey (Maybe ThreadId)
tidTLSKey = unsafePerformIO $ newTLSKey Nothing

mySafeThreadId :: PTM (Maybe ThreadId)
mySafeThreadId = getTLS tidTLSKey


nextThreadNum :: PVar Int
nextThreadNum = unsafePerformIO $ newPVarIO 1

getNextThreadNum :: IO Int
getNextThreadNum = atomically $ do x <- readPVar nextThreadNum
                                   writePVar nextThreadNum (x + 1)
                                   return x

newThreadId :: IO ThreadId
newThreadId = do tnum <- getNextThreadNum
                 tbox <- newPVarIO []
                 prio <- newPVarIO C
                 return (TCB tnum tbox prio)

data Thread = Thread ThreadId SCont
switchT :: (Thread -> PTM Thread) -> IO ()
switchT f = do switch $ \currSC -> do m <- mySafeThreadId
                                      case m of
                                        Nothing      -> return currSC -- refuse to switch if uninitialized
                                        Just currTCB -> do (Thread nextTCB nextSC) <- f (Thread currTCB currSC)
                                                           return nextSC
               checkSignals

blockSignalsKey :: TLSKey Bool
blockSignalsKey = unsafePerformIO $ newTLSKey False

checkSignals :: IO ()
checkSignals = do mx <- atomically $ do
                          mtid <- mySafeThreadId
                          sigsBlocked <- getTLS blockSignalsKey
                          case (mtid, sigsBlocked) of
                              (Nothing, _) -> return Nothing
                              (_, True)    -> return Nothing
                              (Just tid@(TCB _ tbox _), _) -> do
                                  exns <- readPVar tbox
                                  case exns of
                                      (e : es) -> writePVar tbox es >> return (Just e)
                                      []       -> return Nothing
                  case mx of
                    Nothing  -> return ()
                    Just exn -> throw exn

block :: IO a -> IO a
block computation = do
  saved <- atomically $ getTLS blockSignalsKey
  setTLS blockSignalsKey True
  -- Catch synchronous (not asynchronous) exns so we unwind properly
  x <- computation `catch` (\(e :: SomeException) -> setTLS blockSignalsKey saved >> throw e)
  setTLS blockSignalsKey saved
  checkSignals
  return x

unblock :: IO a -> IO a
unblock computation = do
  saved <- atomically $ getTLS blockSignalsKey
  -- Catch synchronous OR asynchronous exns so we unwind properly
  x <- (setTLS blockSignalsKey False >> checkSignals >> computation)
       `catch` (\(e :: SomeException) -> setTLS blockSignalsKey saved >> throw e)
  setTLS blockSignalsKey saved
  return x
