/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "metrics/host_metrics_watcher.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <stdexcept>

namespace metrics {

TEST(DiskStatsParser, ParseDiskStatsGood) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 3838657 2151727147 62187634 0 12832786 96772628 375506 274438 6540552016 27659340 958797 3600058
 259       1 nvme0n1p1 1182 1570 22668 1480 2 0 2 0 0 8279 9695 28 0 8307432 8215 0 0
 259       2 nvme0n1p2 1862 6 283442 252 48 23 544 92 0 260 358 28 0 1348816 13 0 0
 259       3 nvme0n1p3 18583337 9755 1060648508 2128597 10636142 2286095 1057802375 24987800 0 3052155 44045693 224239 165418 3637228912 16929295 0 0
 259       4 nvme0n1p4 1219720 3526 42457635 159682 6416068 266854 267304519 22854958 0 8635715 25491689 37477 28523 1199395944 2477048 0 0
 259       5 nvme0n1p5 6956407 6051 325499183 1035572 8998278 1285685 826619707 14344527 0 1520717 23624869 113734 80497 1694270912 8244768 0 0
 253       0 dm-0 1217871 0 42450873 168534 6682917 0 267304519 81228952 0 9277885 127118565 66000 0 1199395944 45721079 0 0
 252       0 zram0 969732 0 7766128 3504 2911882 0 23295056 17249 0 70594 20753 0 0 0 0 0 0
 253       1 dm-1 18587723 0 1060641794 2394466 12922232 0 1057802375 1126028723 0 3504151 1562828774 389657 0 3637228912 434405585 0 10
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 9);
    ASSERT_EQ(disk_stats["nvme0n1"].size(), 17);
    ASSERT_EQ(disk_stats["nvme0n1"][0], 26762678);
    ASSERT_EQ(disk_stats["nvme0n1"][16], 3600058);

    ASSERT_EQ(disk_stats["dm-1"].size(), 17);
    ASSERT_EQ(disk_stats["dm-1"][0], 18587723);
    ASSERT_EQ(disk_stats["dm-1"][16], 10);
}

TEST(DiskStatsParser, ParseDiskStatsOldKernel) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 3838657 2151727147 62187634 0 12832786 96772628
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 1);
    ASSERT_EQ(disk_stats["nvme0n1"].size(), 17);
    ASSERT_EQ(disk_stats["nvme0n1"][0], 26762678);
    ASSERT_EQ(disk_stats["nvme0n1"][16], 0);
}

TEST(DiskStatsParser, ParseDiskStatsTooOld) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 0);
}

TEST(DiskStatsParser, ParseDiskStatsGarbage) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 asdf 2151727147 62187634 0 12832786 96772628 375506 274438 6540552016 27659340 958797 3600058
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    EXPECT_THROW(
      host_metrics_watcher::parse_diskstats(diskstats, disk_stats),
      std::invalid_argument);
}

TEST(DiskStatsParser, ParseDiskStatsEmpty) {
    std::string_view diskstats = "";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 0);
}

TEST(NetstatParser, ParseNetstatGood) {
    std::string_view netstat
      = R""""(TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPHPHits TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes TCPLossProbeRecovery TCPRenoRecoveryFail TCPSackRecoveryFail TCPRcvCollapsed TCPBacklogCoalesce TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures TCPMemoryPressuresChrono TCPSACKDiscard TCPDSACKIgnoredOld TCPDSACKIgnoredNoUndo TCPSpuriousRTOs TCPMD5NotFound TCPMD5Unexpected TCPMD5Failure TCPSackShifted TCPSackMerged TCPSackShiftFallback TCPBacklogDrop PFMemallocDrop TCPMinTTLDrop TCPDeferAcceptDrop IPReversePathFilter TCPTimeWaitOverflow TCPReqQFullDoCookies TCPReqQFullDrop TCPRetransFail TCPRcvCoalesce TCPOFOQueue TCPOFODrop TCPOFOMerge TCPChallengeACK TCPSYNChallenge TCPFastOpenActive TCPFastOpenActiveFail TCPFastOpenPassive TCPFastOpenPassiveFail TCPFastOpenListenOverflow TCPFastOpenCookieReqd TCPFastOpenBlackhole TCPSpuriousRtxHostQueues BusyPollRxPackets TCPAutoCorking TCPFromZeroWindowAdv TCPToZeroWindowAdv TCPWantZeroWindowAdv TCPSynRetrans TCPOrigDataSent TCPHystartTrainDetect TCPHystartTrainCwnd TCPHystartDelayDetect TCPHystartDelayCwnd TCPACKSkippedSynRecv TCPACKSkippedPAWS TCPACKSkippedSeq TCPACKSkippedFinWait2 TCPACKSkippedTimeWait TCPACKSkippedChallenge TCPWinProbe TCPKeepAlive TCPMTUPFail TCPMTUPSuccess TCPDelivered TCPDeliveredCE TCPAckCompressed TCPZeroWindowDrop TCPRcvQDrop TCPWqueueTooBig TCPFastOpenPassiveAltKey TcpTimeoutRehash TcpDuplicateDataRehash TCPDSACKRecvSegs TCPDSACKIgnoredDubious TCPMigrateReqSuccess TCPMigrateReqFailure TCPPLBRehash TCPAORequired TCPAOBad TCPAOKeyNotFound TCPAOGood TCPAODroppedIcmps
TcpExt: 0 0 0 43 1050 0 0 2 0 0 209179 5 0 0 7171 1252323 354 16308 0 0 11695010 3728741 25084080 0 1010 0 12037 2 517 3 46 352 182 35411 0 29 3 4414 409 58359 10636 1210 0 21 0 287572 16570 159 7265 98 37111 981 0 732 0 0 0 0 289 164 3232 57 0 0 0 4760 6421 8428 0 0 0 0 0 0 0 0 0 2914786 170106 0 161 38 28 0 0 0 0 0 0 0 724 0 382763 674 674 37701 46427 42334561 292 22415 1038 133777 10 3305 1070 0 205 0 253 216961 0 0 42583879 0 69433 0 0 0 0 57406 35 7191 289 0 0 0 0 0 0 0 0
IpExt: InNoRoutes InTruncatedPkts InMcastPkts OutMcastPkts InBcastPkts OutBcastPkts InOctets OutOctets InMcastOctets OutMcastOctets InBcastOctets OutBcastOctets InCsumErrors InNoECTPkts InECT1Pkts InECT0Pkts InCEPkts ReasmOverlaps
IpExt: 0 0 62439 170 8227 0 49809079466 39136412410 13744385 18736 714165 0 0 67558266 0 389832 1 0
MPTcpExt: MPCapableSYNRX MPCapableSYNTX MPCapableSYNACKRX MPCapableACKRX MPCapableFallbackACK MPCapableFallbackSYNACK MPCapableSYNTXDrop MPCapableSYNTXDisabled MPCapableEndpAttempt MPFallbackTokenInit MPTCPRetrans MPJoinNoTokenFound MPJoinSynRx MPJoinSynBackupRx MPJoinSynAckRx MPJoinSynAckBackupRx MPJoinSynAckHMacFailure MPJoinAckRx MPJoinAckHMacFailure MPJoinSynTx MPJoinSynTxCreatSkErr MPJoinSynTxBindErr MPJoinSynTxConnectErr DSSNotMatching DSSCorruptionFallback DSSCorruptionReset InfiniteMapTx InfiniteMapRx DSSNoMatchTCP DataCsumErr OFOQueueTail OFOQueue OFOMerge NoDSSInWindow DuplicateData AddAddr AddAddrTx AddAddrTxDrop EchoAdd EchoAddTx EchoAddTxDrop PortAdd AddAddrDrop MPJoinPortSynRx MPJoinPortSynAckRx MPJoinPortAckRx MismatchPortSynRx MismatchPortAckRx RmAddr RmAddrDrop RmAddrTx RmAddrTxDrop RmSubflow MPPrioTx MPPrioRx MPFailTx MPFailRx MPFastcloseTx MPFastcloseRx MPRstTx MPRstRx RcvPruned SubflowStale SubflowRecover SndWndShared RcvWndShared RcvWndConflictUpdate RcvWndConflict MPCurrEstab Blackhole
MPTcpExt: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
)"""";

    host_metrics_watcher::netstat_stats stats;
    host_metrics_watcher::parse_netstat(netstat, stats);

    ASSERT_EQ(stats.bytes_received, 49809079466);
    ASSERT_EQ(stats.bytes_sent, 39136412410);
}

TEST(NetstatParser, ParseNetstatGarbage) {
    std::string_view netstat
      = R""""(TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPHPHits TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes TCPLossProbeRecovery TCPRenoRecoveryFail TCPSackRecoveryFail TCPRcvCollapsed TCPBacklogCoalesce TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures TCPMemoryPressuresChrono TCPSACKDiscard TCPDSACKIgnoredOld TCPDSACKIgnoredNoUndo TCPSpuriousRTOs TCPMD5NotFound TCPMD5Unexpected TCPMD5Failure TCPSackShifted TCPSackMerged TCPSackShiftFallback TCPBacklogDrop PFMemallocDrop TCPMinTTLDrop TCPDeferAcceptDrop IPReversePathFilter TCPTimeWaitOverflow TCPReqQFullDoCookies TCPReqQFullDrop TCPRetransFail TCPRcvCoalesce TCPOFOQueue TCPOFODrop TCPOFOMerge TCPChallengeACK TCPSYNChallenge TCPFastOpenActive TCPFastOpenActiveFail TCPFastOpenPassive TCPFastOpenPassiveFail TCPFastOpenListenOverflow TCPFastOpenCookieReqd TCPFastOpenBlackhole TCPSpuriousRtxHostQueues BusyPollRxPackets TCPAutoCorking TCPFromZeroWindowAdv TCPToZeroWindowAdv TCPWantZeroWindowAdv TCPSynRetrans TCPOrigDataSent TCPHystartTrainDetect TCPHystartTrainCwnd TCPHystartDelayDetect TCPHystartDelayCwnd TCPACKSkippedSynRecv TCPACKSkippedPAWS TCPACKSkippedSeq TCPACKSkippedFinWait2 TCPACKSkippedTimeWait TCPACKSkippedChallenge TCPWinProbe TCPKeepAlive TCPMTUPFail TCPMTUPSuccess TCPDelivered TCPDeliveredCE TCPAckCompressed TCPZeroWindowDrop TCPRcvQDrop TCPWqueueTooBig TCPFastOpenPassiveAltKey TcpTimeoutRehash TcpDuplicateDataRehash TCPDSACKRecvSegs TCPDSACKIgnoredDubious TCPMigrateReqSuccess TCPMigrateReqFailure TCPPLBRehash TCPAORequired TCPAOBad TCPAOKeyNotFound TCPAOGood TCPAODroppedIcmps
TcpExt: 0 0 0 43 1050 0 0 2 0 0 209179 5 0 0 7171 1252323 354 16308 0 0 11695010 3728741 25084080 0 1010 0 12037 2 517 3 46 352 182 35411 0 29 3 4414 409 58359 10636 1210 0 21 0 287572 16570 159 7265 98 37111 981 0 732 0 0 0 0 289 164 3232 57 0 0 0 4760 6421 8428 0 0 0 0 0 0 0 0 0 2914786 170106 0 161 38 28 0 0 0 0 0 0 0 724 0 382763 674 674 37701 46427 42334561 292 22415 1038 133777 10 3305 1070 0 205 0 253 216961 0 0 42583879 0 69433 0 0 0 0 57406 35 7191 289 0 0 0 0 0 0 0 0
IpExt: InNoRoutes InTruncatedPkts InMcastPkts OutMcastPkts InBcastPkts OutBcastPkts InOctets OutOctets InMcastOctets OutMcastOctets InBcastOctets OutBcastOctets InCsumErrors InNoECTPkts InECT1Pkts InECT0Pkts InCEPkts ReasmOverlaps
IpExt: 0 0 62439 170 8227 0 asdf 39136412410 13744385 18736 714165 0 0 67558266 0 389832 1 0
MPTcpExt: MPCapableSYNRX MPCapableSYNTX MPCapableSYNACKRX MPCapableACKRX MPCapableFallbackACK MPCapableFallbackSYNACK MPCapableSYNTXDrop MPCapableSYNTXDisabled MPCapableEndpAttempt MPFallbackTokenInit MPTCPRetrans MPJoinNoTokenFound MPJoinSynRx MPJoinSynBackupRx MPJoinSynAckRx MPJoinSynAckBackupRx MPJoinSynAckHMacFailure MPJoinAckRx MPJoinAckHMacFailure MPJoinSynTx MPJoinSynTxCreatSkErr MPJoinSynTxBindErr MPJoinSynTxConnectErr DSSNotMatching DSSCorruptionFallback DSSCorruptionReset InfiniteMapTx InfiniteMapRx DSSNoMatchTCP DataCsumErr OFOQueueTail OFOQueue OFOMerge NoDSSInWindow DuplicateData AddAddr AddAddrTx AddAddrTxDrop EchoAdd EchoAddTx EchoAddTxDrop PortAdd AddAddrDrop MPJoinPortSynRx MPJoinPortSynAckRx MPJoinPortAckRx MismatchPortSynRx MismatchPortAckRx RmAddr RmAddrDrop RmAddrTx RmAddrTxDrop RmSubflow MPPrioTx MPPrioRx MPFailTx MPFailRx MPFastcloseTx MPFastcloseRx MPRstTx MPRstRx RcvPruned SubflowStale SubflowRecover SndWndShared RcvWndShared RcvWndConflictUpdate RcvWndConflict MPCurrEstab Blackhole
MPTcpExt: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
)"""";

    host_metrics_watcher::netstat_stats stats;
    EXPECT_THROW(
      host_metrics_watcher::parse_netstat(netstat, stats),
      std::invalid_argument);
}

TEST(NetstatParser, ParseNetstatNoValuesLine) {
    std::string_view netstat
      = R""""(TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPHPHits TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes TCPLossProbeRecovery TCPRenoRecoveryFail TCPSackRecoveryFail TCPRcvCollapsed TCPBacklogCoalesce TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures TCPMemoryPressuresChrono TCPSACKDiscard TCPDSACKIgnoredOld TCPDSACKIgnoredNoUndo TCPSpuriousRTOs TCPMD5NotFound TCPMD5Unexpected TCPMD5Failure TCPSackShifted TCPSackMerged TCPSackShiftFallback TCPBacklogDrop PFMemallocDrop TCPMinTTLDrop TCPDeferAcceptDrop IPReversePathFilter TCPTimeWaitOverflow TCPReqQFullDoCookies TCPReqQFullDrop TCPRetransFail TCPRcvCoalesce TCPOFOQueue TCPOFODrop TCPOFOMerge TCPChallengeACK TCPSYNChallenge TCPFastOpenActive TCPFastOpenActiveFail TCPFastOpenPassive TCPFastOpenPassiveFail TCPFastOpenListenOverflow TCPFastOpenCookieReqd TCPFastOpenBlackhole TCPSpuriousRtxHostQueues BusyPollRxPackets TCPAutoCorking TCPFromZeroWindowAdv TCPToZeroWindowAdv TCPWantZeroWindowAdv TCPSynRetrans TCPOrigDataSent TCPHystartTrainDetect TCPHystartTrainCwnd TCPHystartDelayDetect TCPHystartDelayCwnd TCPACKSkippedSynRecv TCPACKSkippedPAWS TCPACKSkippedSeq TCPACKSkippedFinWait2 TCPACKSkippedTimeWait TCPACKSkippedChallenge TCPWinProbe TCPKeepAlive TCPMTUPFail TCPMTUPSuccess TCPDelivered TCPDeliveredCE TCPAckCompressed TCPZeroWindowDrop TCPRcvQDrop TCPWqueueTooBig TCPFastOpenPassiveAltKey TcpTimeoutRehash TcpDuplicateDataRehash TCPDSACKRecvSegs TCPDSACKIgnoredDubious TCPMigrateReqSuccess TCPMigrateReqFailure TCPPLBRehash TCPAORequired TCPAOBad TCPAOKeyNotFound TCPAOGood TCPAODroppedIcmps
TcpExt: 0 0 0 43 1050 0 0 2 0 0 209179 5 0 0 7171 1252323 354 16308 0 0 11695010 3728741 25084080 0 1010 0 12037 2 517 3 46 352 182 35411 0 29 3 4414 409 58359 10636 1210 0 21 0 287572 16570 159 7265 98 37111 981 0 732 0 0 0 0 289 164 3232 57 0 0 0 4760 6421 8428 0 0 0 0 0 0 0 0 0 2914786 170106 0 161 38 28 0 0 0 0 0 0 0 724 0 382763 674 674 37701 46427 42334561 292 22415 1038 133777 10 3305 1070 0 205 0 253 216961 0 0 42583879 0 69433 0 0 0 0 57406 35 7191 289 0 0 0 0 0 0 0 0
IpExt: InNoRoutes InTruncatedPkts InMcastPkts OutMcastPkts InBcastPkts OutBcastPkts InOctets OutOctets InMcastOctets OutMcastOctets InBcastOctets OutBcastOctets InCsumErrors InNoECTPkts InECT1Pkts InECT0Pkts InCEPkts ReasmOverlaps
MPTcpExt: MPCapableSYNRX MPCapableSYNTX MPCapableSYNACKRX MPCapableACKRX MPCapableFallbackACK MPCapableFallbackSYNACK MPCapableSYNTXDrop MPCapableSYNTXDisabled MPCapableEndpAttempt MPFallbackTokenInit MPTCPRetrans MPJoinNoTokenFound MPJoinSynRx MPJoinSynBackupRx MPJoinSynAckRx MPJoinSynAckBackupRx MPJoinSynAckHMacFailure MPJoinAckRx MPJoinAckHMacFailure MPJoinSynTx MPJoinSynTxCreatSkErr MPJoinSynTxBindErr MPJoinSynTxConnectErr DSSNotMatching DSSCorruptionFallback DSSCorruptionReset InfiniteMapTx InfiniteMapRx DSSNoMatchTCP DataCsumErr OFOQueueTail OFOQueue OFOMerge NoDSSInWindow DuplicateData AddAddr AddAddrTx AddAddrTxDrop EchoAdd EchoAddTx EchoAddTxDrop PortAdd AddAddrDrop MPJoinPortSynRx MPJoinPortSynAckRx MPJoinPortAckRx MismatchPortSynRx MismatchPortAckRx RmAddr RmAddrDrop RmAddrTx RmAddrTxDrop RmSubflow MPPrioTx MPPrioRx MPFailTx MPFailRx MPFastcloseTx MPFastcloseRx MPRstTx MPRstRx RcvPruned SubflowStale SubflowRecover SndWndShared RcvWndShared RcvWndConflictUpdate RcvWndConflict MPCurrEstab Blackhole
MPTcpExt: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
)"""";

    host_metrics_watcher::netstat_stats stats;
    host_metrics_watcher::parse_netstat(netstat, stats);

    ASSERT_EQ(stats.bytes_received, 0);
    ASSERT_EQ(stats.bytes_sent, 0);
}

TEST(NetstatParser, ParseNetstatNoHeaderLine) {
    std::string_view netstat
      = R""""(TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPHPHits TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes TCPLossProbeRecovery TCPRenoRecoveryFail TCPSackRecoveryFail TCPRcvCollapsed TCPBacklogCoalesce TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures TCPMemoryPressuresChrono TCPSACKDiscard TCPDSACKIgnoredOld TCPDSACKIgnoredNoUndo TCPSpuriousRTOs TCPMD5NotFound TCPMD5Unexpected TCPMD5Failure TCPSackShifted TCPSackMerged TCPSackShiftFallback TCPBacklogDrop PFMemallocDrop TCPMinTTLDrop TCPDeferAcceptDrop IPReversePathFilter TCPTimeWaitOverflow TCPReqQFullDoCookies TCPReqQFullDrop TCPRetransFail TCPRcvCoalesce TCPOFOQueue TCPOFODrop TCPOFOMerge TCPChallengeACK TCPSYNChallenge TCPFastOpenActive TCPFastOpenActiveFail TCPFastOpenPassive TCPFastOpenPassiveFail TCPFastOpenListenOverflow TCPFastOpenCookieReqd TCPFastOpenBlackhole TCPSpuriousRtxHostQueues BusyPollRxPackets TCPAutoCorking TCPFromZeroWindowAdv TCPToZeroWindowAdv TCPWantZeroWindowAdv TCPSynRetrans TCPOrigDataSent TCPHystartTrainDetect TCPHystartTrainCwnd TCPHystartDelayDetect TCPHystartDelayCwnd TCPACKSkippedSynRecv TCPACKSkippedPAWS TCPACKSkippedSeq TCPACKSkippedFinWait2 TCPACKSkippedTimeWait TCPACKSkippedChallenge TCPWinProbe TCPKeepAlive TCPMTUPFail TCPMTUPSuccess TCPDelivered TCPDeliveredCE TCPAckCompressed TCPZeroWindowDrop TCPRcvQDrop TCPWqueueTooBig TCPFastOpenPassiveAltKey TcpTimeoutRehash TcpDuplicateDataRehash TCPDSACKRecvSegs TCPDSACKIgnoredDubious TCPMigrateReqSuccess TCPMigrateReqFailure TCPPLBRehash TCPAORequired TCPAOBad TCPAOKeyNotFound TCPAOGood TCPAODroppedIcmps
TcpExt: 0 0 0 43 1050 0 0 2 0 0 209179 5 0 0 7171 1252323 354 16308 0 0 11695010 3728741 25084080 0 1010 0 12037 2 517 3 46 352 182 35411 0 29 3 4414 409 58359 10636 1210 0 21 0 287572 16570 159 7265 98 37111 981 0 732 0 0 0 0 289 164 3232 57 0 0 0 4760 6421 8428 0 0 0 0 0 0 0 0 0 2914786 170106 0 161 38 28 0 0 0 0 0 0 0 724 0 382763 674 674 37701 46427 42334561 292 22415 1038 133777 10 3305 1070 0 205 0 253 216961 0 0 42583879 0 69433 0 0 0 0 57406 35 7191 289 0 0 0 0 0 0 0 0
IpExt: 0 0 62439 170 8227 0 asdf 39136412410 13744385 18736 714165 0 0 67558266 0 389832 1 0
MPTcpExt: MPCapableSYNRX MPCapableSYNTX MPCapableSYNACKRX MPCapableACKRX MPCapableFallbackACK MPCapableFallbackSYNACK MPCapableSYNTXDrop MPCapableSYNTXDisabled MPCapableEndpAttempt MPFallbackTokenInit MPTCPRetrans MPJoinNoTokenFound MPJoinSynRx MPJoinSynBackupRx MPJoinSynAckRx MPJoinSynAckBackupRx MPJoinSynAckHMacFailure MPJoinAckRx MPJoinAckHMacFailure MPJoinSynTx MPJoinSynTxCreatSkErr MPJoinSynTxBindErr MPJoinSynTxConnectErr DSSNotMatching DSSCorruptionFallback DSSCorruptionReset InfiniteMapTx InfiniteMapRx DSSNoMatchTCP DataCsumErr OFOQueueTail OFOQueue OFOMerge NoDSSInWindow DuplicateData AddAddr AddAddrTx AddAddrTxDrop EchoAdd EchoAddTx EchoAddTxDrop PortAdd AddAddrDrop MPJoinPortSynRx MPJoinPortSynAckRx MPJoinPortAckRx MismatchPortSynRx MismatchPortAckRx RmAddr RmAddrDrop RmAddrTx RmAddrTxDrop RmSubflow MPPrioTx MPPrioRx MPFailTx MPFailRx MPFastcloseTx MPFastcloseRx MPRstTx MPRstRx RcvPruned SubflowStale SubflowRecover SndWndShared RcvWndShared RcvWndConflictUpdate RcvWndConflict MPCurrEstab Blackhole
MPTcpExt: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
)"""";

    host_metrics_watcher::netstat_stats stats;
    host_metrics_watcher::parse_netstat(netstat, stats);

    ASSERT_EQ(stats.bytes_received, 0);
    ASSERT_EQ(stats.bytes_sent, 0);
}

TEST(NetstatParser, ParseNetstatEmpty) {
    std::string_view netstat = "";

    host_metrics_watcher::netstat_stats stats;
    host_metrics_watcher::parse_netstat(netstat, stats);

    ASSERT_EQ(stats.bytes_received, 0);
    ASSERT_EQ(stats.bytes_sent, 0);
}

// No need to test edge conditions as it shares the impl with the netstat one
TEST(SnmpParser, ParseSnmpGood) {
    std::string_view netstat
      = R""""(Ip: Forwarding DefaultTTL InReceives InHdrErrors InAddrErrors ForwDatagrams InUnknownProtos InDiscards InDelivers OutRequests OutDiscards OutNoRoutes ReasmTimeout ReasmReqds ReasmOKs ReasmFails FragOKs FragFails FragCreates OutTransmits
Ip: 1 64 47900296 0 0 0 0 0 47899998 40889375 0 764 0 0 0 0 0 0 0 40889375
Icmp: InMsgs InErrors InCsumErrors InDestUnreachs InTimeExcds InParmProbs InSrcQuenchs InRedirects InEchos InEchoReps InTimestamps InTimestampReps InAddrMasks InAddrMaskReps OutMsgs OutErrors OutRateLimitGlobal OutRateLimitHost OutDestUnreachs OutTimeExcds OutParmProbs OutSrcQuenchs OutRedirects OutEchos OutEchoReps OutTimestamps OutTimestampReps OutAddrMasks OutAddrMaskReps
Icmp: 20178 0 0 20178 0 0 0 0 0 0 0 0 0 0 20165 0 0 0 20164 0 0 0 0 1 0 0 0 0 0
IcmpMsg: InType3 OutType3 OutType8
IcmpMsg: 20178 20164 1
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
Tcp: 1 200 120000 -1 346431 163322 1382 2356 12 48326032 56291522 72272 19 105396 0
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
Udp: 677388 20139 0 635596 0 0 0 8037 0
UdpLite: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
UdpLite: 0 0 0 0 0 0 0 0 0
)"""";

    host_metrics_watcher::snmp_stats stats;
    host_metrics_watcher::parse_snmp(netstat, stats);

    ASSERT_EQ(stats.tcp_established, 12);
    ASSERT_EQ(stats.packets_received, 47900296);
    ASSERT_EQ(stats.packets_sent, 40889375);
}

} // namespace metrics
