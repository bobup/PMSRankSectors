#!/usr/bin/perl -w

# RS_RankSectors.pm - a module to rank PMS swimmers into one of 4 possible sectors:
#	- A: those pool competitors that have achieved at least one national qualifying time; 
#	- B: those pool competitors who are within 20% of a national qualifying time; 
#	- C: those pool competitors who haven’t achieved a national qualifying time + 20%;
#	- D: those competitors who compete in open water only.
#
# In order to determine members of sectors A through C this program uses a populated AGSOTY database
# for the appropriate year.  In order to determine members of sector D this program uses a populated 
# Accumulated Points (OW) database for the appropriate year.
#
# NOTE:  this code assumes that we only generate results for "Single Age Group", meaning that if a swimmer
# changes age groups during the season all of their points apply to their oldest age group.
#
# Copyright (c) 2017 Bob Upshaw.  This software is covered under the Open Source MIT License 


package RS_RankSectors;


use strict;
use sigtrap;
use warnings;
use diagnostics;
use Spreadsheet::Read;

use FindBin;
use File::Spec;

use lib File::Spec->catdir( $FindBin::Bin, '..', '..', 'PMSPerlModules' );
require PMSMacros;
require PMSLogging;
require PMS_MySqlSupport;
require PMSConstants;

my $debug = 0;

# We will store all NQT's below in a few hashtables.  We'll compare swimmer's swim times
# with NQTs to determine what "sector" they are in (see comments at the beginning of
# this module.)  If a swimmer doesn't fall into the A sector we'll see if the fall
# into the B sector by comparing their times to a slightly slower NQT.  This slower
# time, known as the 'alternative NQT' below, is a percentage of the NQT, for example
# 120% of the NQT.  This percentage is set during initialization of this module and
# stored here:
my $BSectorPercentage;		# e.g. 120 for 120 percent
my $BSector;				# the multiplier used, e.g. 1.2 for 120%

# hashtables used to store the National Qualifying Times for each course for the
# year being processed.
my %LCM_NQT;		# $LCM_NQT{"$eventId.$ageGroup.$gender"} = DURATION
					# where:  $eventId designates the event (e.g. 50 free), and
					# where:  $ageGroup is one of the masters age groups (e.g. "55-59"), and
					# where:  $gender is the gender (WOMEN,MEN)
					# DURATION is the qualifying time in hundredths of a second (an integer)
					# NOTE; if $LCM_NQT is not defined then there is no qualifying time for
					#	that event and age group.  In this case every time qualifies.
my $LCM_NQT_Ref = \%LCM_NQT;

my %SCM_NQT;		# $SCM_NQT{"$eventId.$ageGroup.$gender"} = DURATION
					# (same idea as for %LCM_NQT)
my $SCM_NQT_Ref = \%SCM_NQT;

my %SCY_NQT;		# $SCY_NQT{"$eventId.$ageGroup.$gender"} = DURATION
					# (same idea as for %LCM_NQT)
my $SCY_NQT_Ref = \%SCY_NQT;

# NOTES on above NQTs:
# To convert a time of the form hh:mm:ss.hh into an integer (used above) see 
#		PMSUtil::GenerateCanonicalDurationForDB()
# To convert the textual name of an event (e.g. "100 free") into an eventId see
#		PMSUtil::GetDistanceAndStroke()
# and then see TT_MySqlSupport::AddNewEventIfNecessary() to see how to convert a
# {distance,stroke} into an eventId.

# hashtable to remember the reason we assigned each swimmer their sector.  We only remember
# the cases where sector A or B was assigned.  The others are obvious.
my %sectorReason;			# $sectorReason{$swimmerId.sector} = "A" or "B"
							# $sectorReason{$swimmerId.course} = LCM, SCM, or
							# $sectorReason{$swimmerId.eventId} = specifies the event
							# $sectorReason{$swimmerId.ageGroup} = 18-24, 25-29, ...
							# $sectorReason{$swimmerId.duration} = swim time in ms
							# $sectorReason{$swimmerId.nqtDuration} = the National Qualifying Time
							# $sectorReason{$swimmerId.additionalDuration} = the National Qualifying Time + x%
							# $sectorReason{$swimmerId.diff} = difference (see below)
# If $sectorReason{$swimmerId.sector} is "A" then 'diff' is the difference between the 'duration' chosen
# and the corresponding NQT, in ms.  The 'duration' is always <= NQT.
# If $sectorReason{$swimmerId.sector} is "B" then 'diff' is the difference between the 'duration' chosen 
# and the corresponding 'additionalDuration', in ms.  The 'duration' is always > NQT and < 'additionalDuration'.
# If $sectorReason{$swimmerId.sector} is "C" then 'diff' is the difference between the 'duration' chosen
# and the corresponding additionalDuration, in ms.  The 'duration' is always > 'additionalDuration' where
# the chosen 'diff' is the smallest diff for all this swimmer's 'duration's..





sub RankAllSwimmersIntoExactlyOneSector( ) {
	my ($countSwimmers, $firstName, $middleInitial, $lastName, $gender, $swimmerId, $ageGroup1, $ageGroup2);
	my ($sth, $rv);
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $query;
	
	PMSLogging::PrintLog( "", "", "** Begin RankAllSwimmersIntoExactlyOneSector", 1 );

	
	# pass through the list of all PAC swimmers who have competed this year and analyze the swims for each
	# swimmer to determine their sector:
	$query = "SELECT FirstName,MiddleInitial,LastName,Gender,SwimmerId,AgeGroup1,AgeGroup2 " .
		"FROM Swimmer";
	
	($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
	$countSwimmers = 1;
	while( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		$countSwimmers++;
		$firstName = $resultHash->{'FirstName'};
		$middleInitial = $resultHash->{'MiddleInitial'};
		$lastName = $resultHash->{'LastName'};
		$gender = $resultHash->{'Gender'};
		$gender = ($gender eq "F" ? "WOMEN" : "MEN");
		$swimmerId = $resultHash->{'SwimmerId'};
		$ageGroup1 = $resultHash->{'AgeGroup1'};
		$ageGroup2 = $resultHash->{'AgeGroup2'};
		
		if( ($countSwimmers % 500) == 0) {
			print "  ...$countSwimmers...\n";
		}
		
		# next, find all PAC pool splashes for this swimmer.
		# (We only look at the PAC splashes and not USMS splashes, because any swim that resulted in a USMS
		# top ten splash will obviously result in a top 10 PAC splash.)
		my ($sth2, $rv2);
		$query = "SELECT SplashId, Course, Duration, EventId, AgeGroup FROM Splash " .
			"WHERE SwimmerId = $swimmerId and ((Course = 'SCY') OR (Course = 'SCM') OR (Course = 'LCM')) " .
			"AND Org = 'PAC'";
		($sth2, $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
		my $sector = 0;
		my $thisSwimmerHasAPoolTime = 0;		# set to 1 if they have at least one pool swim
		my $thisSwimmerHasBTimes = 0;			# set to 1 if they have at least one 'B' sector time
		while( defined(my $resultHash2 = $sth2->fetchrow_hashref) ) {
			$thisSwimmerHasAPoolTime = 1;
			my $course = $resultHash2->{'Course'};
			my $eventId = $resultHash2->{'EventId'};
			my $ageGroup = $resultHash2->{'AgeGroup'};
			my $duration = $resultHash2->{'Duration'};
			my $NQT_Ref = $LCM_NQT_Ref;
			if( $course eq "SCY" ) {
				$NQT_Ref = $SCY_NQT_Ref;
			} elsif( $course eq "SCM" ) {
				$NQT_Ref = $SCM_NQT_Ref;
			}
			my $nqtDuration = $NQT_Ref->{"$eventId.$ageGroup.$gender"};
			my $nqtDurationStr = $nqtDuration;
			if( !defined $nqtDuration ) {
				$nqtDurationStr =  "(NO TIME)";
				$nqtDuration = 0;
			}
			if( (!$nqtDuration) ||
				($duration <= $nqtDuration) ) {
				$sector = "A";
				RecordSectorReason( $swimmerId, $sector, $course, $eventId, $ageGroup, $duration, 
					$nqtDuration, $nqtDurationStr );
				last; 	# we don't need to look at any more times for this swimmer
			}
			my $additionalDuration = $nqtDuration * $BSector;
			if( $duration <= $additionalDuration ) {
				$sector = "B";
				$thisSwimmerHasBTimes = 1;
				RecordSectorReason( $swimmerId, $sector, $course, $eventId, $ageGroup, $duration, 
					$nqtDuration, $nqtDurationStr, $additionalDuration );
			} else {
				# if this swimmer has no 'B' times then we'll assume, for the moment, that they
				# will be a 'C' swimmer.  This will change if we find a time that puts them
				# into the 'A' or 'B' sector:
				if( !$thisSwimmerHasBTimes ) {
					$sector = "C";
					RecordSectorReason( $swimmerId, $sector, $course, $eventId, $ageGroup, $duration, 
						$nqtDuration, $nqtDurationStr, $additionalDuration );
				}
			}
		} # end of while( defined(my $resultHash2 =...
		if( !$sector ) {
			# This swimmer had no pool swims.  Thus, all of their swims must have been OW
			$sector = "D";
		}
		$query = "UPDATE Swimmer SET SECTOR = '$sector' WHERE SwimmerId = $swimmerId";
		($sth2, $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
		RecordSectorReason( $swimmerId );
	} # end of while( defined(my $resultHash =...

	PMSLogging::PrintLog( "", "", "** End RankAllSwimmersIntoExactlyOneSector", 1 );

} # end of RankAllSwimmersIntoExactlyOneSector()

use File::Basename;


# 				RecordSectorReason( $swimmerId, $sector, $course, $eventId, $ageGroup, $duration, 
#					$nqtDuration, $nqtDurationStr, $additionalDuration );
# all but swimmerid are optional
# sector is A, B, or C  (not D)
sub RecordSectorReason( ) {
	my ($swimmerId, $sector, $course, $eventId, $ageGroup, $duration, $nqtDuration, 
		$nqtDurationStr, $additionalDuration) = @_;
	my $query;
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	if( !defined $additionalDuration ) {
		$additionalDuration = 0;
	}
	if( defined $sector) {
		# record details for a specific swimmer:
		# (In this case all parameters passed to this routine are defined with the 
		# exception of $additionalDuration (=0) if the passed $sector is "A")
		if( $sector eq "A" ) {
			$sectorReason{"$swimmerId.sector"} = $sector;
			$sectorReason{"$swimmerId.course"} = $course;
			$sectorReason{"$swimmerId.eventId"} = $eventId;
			$sectorReason{"$swimmerId.ageGroup"} = $ageGroup;
			$sectorReason{"$swimmerId.duration"} = $duration;
			$sectorReason{"$swimmerId.nqt"} = $nqtDuration;
			$sectorReason{"$swimmerId.nqtDuration"} = $nqtDurationStr;
			$sectorReason{"$swimmerId.diff"} = $nqtDuration - $duration;
		} elsif( $sector eq "B" ) {
			$sectorReason{"$swimmerId.sector"} = $sector;
			$sectorReason{"$swimmerId.course"} = $course;
			$sectorReason{"$swimmerId.eventId"} = $eventId;
			$sectorReason{"$swimmerId.ageGroup"} = $ageGroup;
			$sectorReason{"$swimmerId.duration"} = $duration;
			$sectorReason{"$swimmerId.nqt"} = $nqtDuration;
			$sectorReason{"$swimmerId.nqtDuration"} = $nqtDurationStr;
			$sectorReason{"$swimmerId.additionalDuration"} = $additionalDuration;
			$sectorReason{"$swimmerId.diff"} = $additionalDuration - $duration;
		} else {
			# this is a 'C' sector:
			my $diff =  $duration - $additionalDuration;
			if( !defined $sectorReason{"$swimmerId.diff"} || 
				($diff <  $sectorReason{"$swimmerId.diff"}) ) {
				$sectorReason{"$swimmerId.sector"} = $sector;
				$sectorReason{"$swimmerId.course"} = $course;
				$sectorReason{"$swimmerId.eventId"} = $eventId;
				$sectorReason{"$swimmerId.ageGroup"} = $ageGroup;
				$sectorReason{"$swimmerId.duration"} = $duration;
				$sectorReason{"$swimmerId.nqt"} = $nqtDuration;
				$sectorReason{"$swimmerId.nqtDuration"} = $nqtDurationStr;
				$sectorReason{"$swimmerId.additionalDuration"} = $additionalDuration;
				$sectorReason{"$swimmerId.diff"} = $diff;
			}
		}
			  
	} else {
		# (In this case only the $swimmerId is passed - all other parameters are undefined)
		if( defined $sectorReason{"$swimmerId.sector"} ) {
			# store this swimmer's Sector details
			
#PMSLogging::DumpHash( \%sectorReason, "RecordSectorReason-$swimmerId", 0, 1 );
			
			
			my $sector = $sectorReason{"$swimmerId.sector"};
			my $course = $sectorReason{"$swimmerId.course"};
			my $ageGroup = $sectorReason{"$swimmerId.ageGroup"};
			my $eventName = "?";
			my $durationStr = $sectorReason{"$swimmerId.duration"};
			my $nqtDurationStr = $sectorReason{"$swimmerId.nqtDuration"};
			my $additionalDurationStr = $sectorReason{"$swimmerId.additionalDuration"};
			my $diff = $sectorReason{"$swimmerId.diff"};
			my $detailsStr = "?";
			# get event details:
			$query = "SELECT EventName FROM Event WHERE EventId = " . $sectorReason{"$swimmerId.eventId"};
			my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
		 	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		 		$eventName = $resultHash->{'EventName'};
		 	}
		 	# get the swimmer's time for this event as a string:
			$durationStr = PMSUtil::GenerateDurationStringFromHundredths( $durationStr ) .
				" (" . $durationStr . ")";
			# get the NQT for this event as a string:
			if( $nqtDurationStr ne "(NO TIME)" ) {
				$nqtDurationStr = PMSUtil::GenerateDurationStringFromHundredths( $nqtDurationStr ) .
					" (" . $nqtDurationStr . ")";
			}
			if( ($sector eq "B") || ($sector eq "C") ) {
				# get the "additional" qualifying time for this swimmer as a string:
				$additionalDurationStr = PMSUtil::GenerateDurationStringFromHundredths( $additionalDurationStr ) .
					" (" . $additionalDurationStr . ")";
			}
			if( $sector eq "A" ) {
				$detailsStr = "For example, this swimmer swam the $ageGroup $course $eventName in " .
					"$durationStr, beating the NQT " .
					"of $nqtDurationStr by $diff ms.";
			} elsif( $sector eq "B" ) {
				$detailsStr = "For example, this swimmer swam the $ageGroup $course $eventName in " .
					"$durationStr, slower than the NQT " .
					"of $nqtDurationStr but faster than the alternative $additionalDurationStr " .
					"($BSectorPercentage% of the NQT) by $diff ms.";
			} else {
				# sector == C
				$detailsStr = "For example, this swimmer swam the $ageGroup $course $eventName in " .
					"$durationStr, slower than the NQT " .
					"of $nqtDurationStr and also slower than the alternative $additionalDurationStr " .
					"($BSectorPercentage% of the NQT) by $diff ms.  This is the closest they got to " .
					"the alternative NQT.";
			}
			# add this detail string into the swimmer's record:
			
			$query = "UPDATE Swimmer SET SectorReason = \"$detailsStr\" WHERE SwimmerId = $swimmerId";
			my $rowsAffected = $dbh->do( $query );
			if( $rowsAffected == 0 ) {
				# update failed - 
				PMSLogging::DumpError( "", "", "RS_RankSectors::RecordSectorReason(): Update of Swimmer $swimmerId failed!!", 1 ) if( $debug > 0);
			}
			
		}
	}
} # end of RecordSectorReason()







#	RS_RankSectors::InitializeAllQualifyingTimes( $yearBeingProcessed, 140 );
sub InitializeAllQualifyingTimes( $$ ) {
	my ($yearBeingProcessed, $BPercent) = @_;
	
	PMSLogging::PrintLog( "", "", "** Begin InitializeAllQualifyingTimes ($yearBeingProcessed)", 1 );

	# store our multiplier used to compute an 'alternative NQT':
	$BSectorPercentage = $BPercent;
	$BSector = $BPercent/100;
	
	my $sourceDir = dirname( __FILE__ );
	$sourceDir = "$sourceDir/../NationalQualifyingTimes-$yearBeingProcessed";
	InitializeQualifyingTimes( "SCY", "$sourceDir/SCY_NQTs.xlsx", $SCY_NQT_Ref);
#PMSLogging::DumpHash( $SCY_NQT_Ref, "SCY NQT", 0, 1);
	InitializeQualifyingTimes( "LCM", "$sourceDir/LCM_NQTs.xlsx", $LCM_NQT_Ref );
#PMSLogging::DumpHash( $LCM_NQT_Ref, "LCM NQT", 0, 1);
	
	# for now we'll use the LCM NQTs as the SCM NQTs:
	$SCM_NQT_Ref = $LCM_NQT_Ref;
	PMSLogging::PrintLog( "", "", "** End InitializeAllQualifyingTimes ($yearBeingProcessed)", 1 );
} # end of InitializeAllQualifyingTimes();
	
	
	
sub InitializeQualifyingTimes( $$$ ) {
	my ($course, $NQTFullFileName, $QTRef) = @_;
    my $g_ref = ReadData( $NQTFullFileName );
    # are we dealing with Yards or Meters?
    my $unit = "Meter";
    if( $course eq "SCY" ) {
    	$unit = "Yard";
    }
    # $g_ref is an array reference
    # $g_ref->[0] is a reference to a hashtable:  the "control hash"
    my $numSheets = $g_ref->[0]{sheets};        # number of sheets, including empty sheets
    print "\nfile $NQTFullFileName:\n  Number of sheets:  $numSheets.\n  Names of non-empty sheets:\n" 
    	if( $debug > 0);
    my $sheetNames_ref = $g_ref->[0]{sheet};  # reference to a hashtable containing names of non-empty sheets.  key = sheet
                                              # name, value = monotonically increasing integer starting at 1 
    my %tmp = % { $sheetNames_ref } ;         # hashtable of sheet names (above)
    my ($sheetName);
    foreach $sheetName( sort { $tmp{$a} <=> $tmp{$b} } keys %tmp ) {
        print "    $sheetName\n" if( $debug > 0 );
    }
    # get the first sheet
    my $g_sheet1_ref = $g_ref->[1];         # reference to the hashtable representing the sheet
    my $numRowsInSpreadsheet = $g_sheet1_ref->{maxrow};	# number of rows in NQT file
    my $numColumnsInSpreadsheet = $g_sheet1_ref->{maxcol};
    print "numRows=$numRowsInSpreadsheet, numCols=$numColumnsInSpreadsheet\n" if( $debug > 0 );
	# Finally, pass through the sheet collecting the national qualifying times:
	my $rowNum;
	my $gender = undef;
	for( $rowNum = 1; $rowNum <= $numRowsInSpreadsheet; $rowNum++ ) {
		# extract data from the spreadsheet:
		my $colA = uc($g_sheet1_ref->{"A$rowNum"});
#print "$course: Row $rowNum, Column A: $colA\n";
		# is this a change in gender?
		if( ($colA eq "WOMEN") || ($colA eq "MEN") ) {
			$gender = $colA;
		} elsif( (defined $gender) && ($colA =~ m/^\d/) ) {
			# we have an event - compute eventId
			#print "$course: Row $rowNum: $gender:$colA\n";
			my($distance, $stroke) = PMSUtil::GetDistanceAndStroke( $colA );
			my $eventId = GetEventIdFromDistanceStroke( $distance, $stroke, $unit );
			if( $eventId != 0 ) {
				# parse the row and populate our hashtable
				GetNQTsForThisEvent( $g_sheet1_ref, $rowNum, $eventId, $gender, $QTRef);
			} else {
				# we couldn't recognize this event - log it and go on
				PMSLogging::DumpError( "", $rowNum, "RS_RankSectors::InitializeQualifyingTimes(): " .
					"Failed to recognize this event: distance=$distance, stroke=$stroke, unit=$unit", 1 );
			}
		}
	} # end of for( $rowNum...
	
	
} # end of InitializeQualifyingTimes()



# 				GetNQTsForThisEvent( $g_sheet1_ref, $rowNum, $eventId, $gender, $QTRef);
sub GetNQTsForThisEvent( $$$$$ ) {
	my ($sheetRef, $rowNum, $eventId, $gender, $QTRef) = @_;
	
	# march across the passed row collecting the value in each column.  That value
	# is stored as the NQT of this event for this gender.  This routine assumes
	# a fixed number of age groups covered by the row:
	for( my $colNum = 2; $colNum <= 14; $colNum++ ) {
		my $colLetter = chr( ord("A") + $colNum -1 );
		my $time = $sheetRef->{"$colLetter$rowNum"};
		next if( $time eq "NO TIME" );
		my $ageGroup = $PMSConstants::AGEGROUPS_MASTERS[$colNum-2];
		$QTRef->{"$eventId.$ageGroup.$gender"} = TT_Util::GenerateCanonicalDurationForDB(
			$time, "", $rowNum );
#print "colLetter=$colLetter, time=$time, ageGroup=$ageGroup\n";
		



#print "time=$time, QT{$eventId.$ageGroup.$gender} = " . $QTRef->{"$eventId.$ageGroup.$gender"} . "\n";


	} 
} # end of GetNQTsForThisEvent()

# return 0 if we can't figure out the event
sub GetEventIdFromDistanceStroke( $$$ ) {
	my ($distance, $stroke, $units) = @_;
	# get ready to use our database:
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $eventId = 0;		# assume failure...
	
	# Look up this event in our database:
	my $query = "SELECT EventId FROM Event WHERE Distance='$distance' AND Units='$units' " .
		"AND Stroke='$stroke'";
	my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		# found it! - get the db id
		$eventId = $resultHash->{'EventId'};
	}
	
	return $eventId;

} # end of GetEventIdFromDistanceStroke()
	
	
1;  # end of RS_RankSectors.pm