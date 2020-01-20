#!/usr/bin/perl -w
# RS_MySqlSupport.pm - support routines and values used by the MySQL based code.

# Copyright (c) 2019 Bob Upshaw and Pacific Masters.  This software is covered under the Open Source MIT License 

package RS_MySqlSupport;

use strict;
use sigtrap;
use warnings;

use DBI;

use FindBin;
use File::Spec;
use File::Basename;
use lib File::Spec->catdir( $FindBin::Bin, '..',  '..', 'PMSPerlModules' );
require PMSUtil;
require PMS_MySqlSupport;
require PMSLogging;


# list of tables that we expect in our db:
my $rsTableListInitialized = 0;		# set to 1 when we've initialized the %tableList with existing tables
my %rsTableList = (
	'Sectors' => 0,
);
# list of tables that we never drop - in order to regenerate them they must be dropped by hand.
my @rsTableListNotDropped = (
	);

#***************************************************************************************************
#****************************** Rank Sectors MySql Support Routines *************************************
#***************************************************************************************************




# InitializeRSDB - get handle to our db; create tables if they are not there.
#
# Call this before trying to use the database.
# Before calling this be sure to drop any tables you'd want created fresh before a run, and
#	call PMS_MySqlSupport::SetSqlParameters() to establish the database parameters.
#
sub InitializeRSDB() {
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $sth;
	my $rv;
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
	
	if( $dbh ) {
		# get our database parameters
		PMS_MySqlSupport::GetTableList( \%rsTableList, \$rsTableListInitialized );
    	foreach my $tableName (keys %rsTableList) {
    		if( ! $rsTableList{$tableName} ) {
    			print "Table '$tableName' does not exist - creating it.\n";

### Sectors
    			if( $tableName eq "Sectors" ) {
		    		($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
    		    		"CREATE TABLE Sectors (SectorsId INT AUTO_INCREMENT PRIMARY KEY, " .
    		    		"Year Char(4), " .
		    			"SwimmerId INT References Swimmer(SwimmerId), " .
		    			"Sector Char(1), " .
    		    		"Course Varchar(15), " .
  						"EventId INT References Event(EventId), " .
		    			"AgeGroup Varchar(10), " .
		    			"Duration INT DEFAULT 0 )" );
    			} else {
    				print "RS_MySqlSupport::InitializeRSDB(): unknown tablename: '$tableName' - ABORT!";
    				exit(1);		# thus must never happen!
    			}
			}
		} # end of foreach(...)
	}
	return $dbh;
} # end of InitializeRSDB()





# DropRSTables - drop (almost) all (existing) Top10 tables in our db
#
sub DropRSTables() {
	PMS_MySqlSupport::GetTableList( \%rsTableList, \$rsTableListInitialized );
	PMS_MySqlSupport::DropTables( \%rsTableList, \@rsTableListNotDropped);
} # end of DropRSTables()



# TT_MySqlSupport::DropTable( $tableName );
# DropTable - drop the passed table in our db
#
sub DropTable( $ ) {
	my $tableName = $_[0];
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $qry;
	my $gotTableToDrop = 0;
	
	# construct the DROP TABLES query:
	PMS_MySqlSupport::GetTableList( \%rsTableList, \$rsTableListInitialized );
	if( $rsTableList{$tableName} ) {
		print "Table '$tableName' exists - dropping it.\n";
		$qry = "DROP table $tableName";
		$gotTableToDrop = 1;
		# update our cache to show that this table doesn't exist
		$rsTableList{$tableName} = 0;
	}
	
	if( $gotTableToDrop ) {
		# Execute the DROP query
		my $sth = $dbh->prepare( $qry ) or 
	    	die "Can't prepare in DropTable(): '$qry'\n";
	    my $rv;
	    $rv = $sth->execute or 
	    	die "Can't execute in DropTable(): '$qry'\n"; 
	}   
} # end of DropTable()


# my $eventId = RS_MySqlSupport::AddNewEventIfNecessary( $distance, $eventCourse,
#	$stroke );
# AddNewEventIfNecessary - look up the passed event in the Event table.  If not found
#	then add the event.  In all cases return the EventId.
#
# PASSED:
#	$distance
#	$units -
#	$stroke -
#	$eventName - (optional)
#
# RETURNED:
#	$eventId -
#
sub AddNewEventIfNecessary($$$) {
	my ($distance, $units, $stroke, $eventName) = @_;
# handle old case (temp)
if( !defined $units ) {
	$eventName = $distance;
	$units = "xxx";
	$stroke = $distance;
	$distance="xxx";
}
	if( !defined $eventName ) {
		$eventName = "$distance $units $stroke";
	}
	my $eventId = 0;
	my $resultHash;
	
	# get ready to use our database:
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	
	# populate the Event table with this event if it's not already there...
	# is this event already in our db?  If so don't try to put it in again
	my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT EventId FROM Event WHERE Distance='$distance' AND Units='$units' " .
		"AND Stroke='$stroke'" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		# this event is already in our DB - get the db id
		$eventId = $resultHash->{'EventId'};
	} else {
		# insert this event
		($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
			"INSERT INTO Event " .
				"(Distance,Units,Stroke,EventName) " .
				"VALUES ('$distance','$units','$stroke','$eventName')") ;
				
		# get the EventId of the event we just entered into our db
    	$eventId = $dbh->last_insert_id(undef, undef, "Event", "EventId");
    	die "Can't determine EventId of newly inserted Event" if( !defined( $eventId ) );
	}
	
	return $eventId;
} # end of AddNewEventIfNecessary()



# AddNewSwimmerIfNecessary - look up the passed swimmer in the Swimmer table.  If not found
#	then add the swimmer.  If found update the ageGroup2 field if necessary.
#	In all cases return the SwimmerId.
#
# We look up the swimmer by reg num.
#
# If the swimmer is found then do the following checks:
#	- first, middle, and last names match
#	- gender in db match passed gender
#	- ageGroup1 or ageGroup2 in db matches passed $ageGroup or is one age group away.
#	- team matches
#
# hack:
sub AddNewSwimmerIfNecessary( $$$$$$$$$$ ){
	my($fileName, $lineNum, $firstName, $middleInitial, $lastName, $gender, $regNum, $age, 
		$ageGroup, $team) = @_;
	my $swimmerId = 0;
	my $resultHash;
	my $ageGroup1 = "";
	my $ageGroup2 = "";
	
	my $debugLastName = "xxxxx";
	
	# make sure the gender is either M or F
	$gender = PMSUtil::GenerateCanonicalGender( $fileName, $lineNum, $gender );
	
	# get ready to use our database:
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	
	# Get the USMS Swimmer id, e.g. regnum 384x-abcde gives us 'abcde'
	my $regNumRt = PMSUtil::GetUSMSSwimmerIdFromRegNum( $regNum );
	
	# populate the Swimmer table with this swimmer if it's not already there...
	# is this swimmer already in our db?  If so don't try to put it in again
	my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT SwimmerId,FirstName,MiddleInitial,LastName,Gender,AgeGroup1,AgeGroup2,RegisteredTeamInitials " .
		"FROM Swimmer WHERE RegNum LIKE \"38%-$regNumRt\"", 
		$debugLastName eq $lastName ? "Looking For > $firstName $lastName":"" );
	$resultHash = $sth->fetchrow_hashref;
	if( $debugLastName eq $lastName ) {
		if( defined($resultHash) ) {
			PMSLogging::PrintLog( "", "", "Williams found with $regNumRt\n", 1 );
		} else {
			PMSLogging::PrintLog( "", "", "Williams NOT found with $regNumRt\n", 1 );
		}
	}
	if( defined($resultHash) ) {
		# this swimmer is already in our DB - get the db id
		$swimmerId = $resultHash->{'SwimmerId'};
		# validate db data
		# first, the age groups for this swimmer is a special case...they can be in 2 age groups for the year
		$ageGroup1 = $resultHash->{'AgeGroup1'};
		$ageGroup2 = $resultHash->{'AgeGroup2'};	# can be empty string
		if( $ageGroup ne $ageGroup1 ) {
			# the passed ageGroup is not the same as the first age group we saw for this swimmer -
			# Do they have a second age group in the DB, and, if so, is it the same as the passed age group?
			if( $ageGroup2 ne "" ) {
				if( $ageGroup ne $ageGroup2 ) {
					PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
						"AgeGroup in results ($ageGroup) != db (\"$ageGroup1\", \"$ageGroup2\") " .
						"for regNum $regNum", 1 );
				} else {
					# the passed age group = ageGroup2 for this swimmer.  Good.
				}
			} else {
				# this swimmer doesn't have a second age group in the DB - make sure the one passed is one
				# age group above or below the current one in the db, and if it is, make it their second
				# age group in the db.
				if( AgeGroupsClose( $ageGroup, $ageGroup1 ) ) {
					# update this swimmer by adding their ageGroup2
					my ($sth2, $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
						"UPDATE Swimmer SET AgeGroup2 = '$ageGroup' " .
						"WHERE SwimmerId = $swimmerId" );
#					$total2AgeGroups++;
#					$MultiAgeGroups{$swimmerId} = "$ageGroup1:$ageGroup:$gender";
				} else {
					# the second age group for this swimmer isn't right - display error
					# and don't add it to the db:
					PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
						"AgeGroup in results ($ageGroup) is not near the ageGroup1 in the db " .
						"(\"$ageGroup1\") " .
						"for regNum $regNum", 1 );
				}
			}
		}

		PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
			"Firstname in results ('$firstName') != db (Swimmer table) ('$resultHash->{'FirstName'}') for regNum $regNum. " .
			"(non-fatal)\n" ) 
			if( lc($firstName) ne lc($resultHash->{'FirstName'}) );
		PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
			"MiddleInitial in results ($middleInitial) != db (Swimmer table) ($resultHash->{'MiddleInitial'}) for regNum $regNum. " .
			"(non-fatal)\n" )
			if( (lc($middleInitial) ne lc($resultHash->{'MiddleInitial'})) && 
				($middleInitial ne "") );
		PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
			"LastName in results (\"$lastName\") != db (Swimmer table) (\"$resultHash->{'LastName'}\") for regNum $regNum. " .
			"(non-fatal)\n" )
			if( lc($lastName) ne lc($resultHash->{'LastName'}) );
		PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
			"Gender in results ($gender) != db (Swimmer table) ($resultHash->{'Gender'}) for regNum $regNum. " .
			"(non-fatal)\n" )
			if( lc($gender) ne lc($resultHash->{'Gender'}) );
			
		PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): ('$fileName', $lineNum): " .
			"Team in results ($team) != db (Swimmer table) ($resultHash->{'RegisteredTeamInitials'}) for regNum $regNum. " .
			"(non-fatal)\n" )
			if( ($team ne "") && (lc($team) ne lc($resultHash->{'RegisteredTeamInitials'})) );
	} else {
		if( 1 ) {
			# see if we have a situation where we have two completely different reg numbers for the
			# same person (a "normal" reg number and one or more vanity reg numbers)
			($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
				"SELECT SwimmerId,FirstName,MiddleInitial,LastName,Gender,AgeGroup1,AgeGroup2," .
				"RegisteredTeamInitials,RegNum " .
				"FROM Swimmer WHERE LastName=\"$lastName\" AND FirstName=\"$firstName\"" );
			while( defined($resultHash = $sth->fetchrow_hashref) ) {
				# this swimmer appears to already in our DB - get the db id
				$swimmerId = $resultHash->{'SwimmerId'};
				$ageGroup1 = $resultHash->{'AgeGroup1'};
				$ageGroup2 = $resultHash->{'AgeGroup2'};	# can be empty string
				my $dbFirstName = $resultHash->{'FirstName'};
				my $dbMiddleInitial = $resultHash->{'MiddleInitial'};
				my $gender = $resultHash->{'Gender'};
				my $regTeam = $resultHash->{'RegisteredTeamInitials'};
				my $dbRegNum = $resultHash->{'RegNum'};
				PMSLogging::DumpWarning( "", "", "TT_MySqlSupport::AddNewSwimmerIfNecessary(): Possible multiple RegNums:\n" .
					"  Can't find '$firstName' '$middleInitial' '$lastName' with regnum '$regNum'," .
					" gender=$gender, ageGroup=$ageGroup, team=$regTeam in the SWIMMER table " .
					"\n  However, found: '$dbFirstName' '$dbMiddleInitial' " .
					"'$lastName' with regnum '$dbRegNum'," .
					" gender=$gender, swimmerId=$swimmerId, ageGroup1=$ageGroup1, ageGroup2=$ageGroup2, " .
					"team=$regTeam in the SWIMMER table." .
					"\n  '$firstName' '$middleInitial' '$lastName' with regnum '$regNum' will be inserted.");
			}
		}
		# Carry on...add this swimmer to our db (even if it's a possible duplicate since we can't
		# know for sure)
		($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
			"INSERT INTO Swimmer " .
				"(FirstName,MiddleInitial,LastName,Gender,RegNum,Age1,Age2,AgeGroup1,RegisteredTeamInitials) " .
				"VALUES (\"$firstName\",\"$middleInitial\",\"$lastName\",\"$gender\",\"$regNum\"," .
				"\"$age\",\"$age\",\"$ageGroup\",\"$team\")") ;
				
		# get the SwimmerId of the swimmer we just entered into our db
    	$swimmerId = $dbh->last_insert_id(undef, undef, "Swimmer", "SwimmerId");
    	die "Can't determine SwimmerId of newly inserted Swimmer" if( !defined( $swimmerId ) );
	}
	
	return $swimmerId;
	
} # end of AddNewSwimmerIfNecessary()





#  MySqlEscape( $string )
# MySqlEscape - escape imbedded quotes in the passed string making the returned
#	string acceptable as a value in a SQL INSERT statement
sub MySqlEscape( $ ) {
	my $string = $_[0];
	$string =~ s/"/\\"/g;
	$string =~ s/\\/\\/g;
	return $string;
} # end of MySqlEscape()




1;  # end of module
