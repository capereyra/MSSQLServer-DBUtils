# package Trigger
# By: Carlos Pereyra
# 08/01/01
# Functions to get trigger information.
# Usage:
# Set DBHANDLE (call sub dbh with valid database handle)
# Set FQN (call sub fq_name with fully qualified name of the trigger as in database.owner.procname)
# 
# Public Methods:
# 
# exists()           returns 1 if FQN procedure exists
# created()          returns create date for proc
# text()             returns the procedure code
#
# Private Methods:
#
# _FQN_elements()    returns the individual parts of the fully qualified name
# _db_query()        returns the result of the SQL statements issued by the public methods.
#
#


package Trigger;
use strict;
use DBI;


sub new {
   my $self  = {};
   bless($self);
}


sub dbh()
{
   my $self = shift;
   if (@_) { $self->{DBHANDLE} = shift; }
   return $self->{DBHANDLE};
}   


sub fq_name()
{
  my $self = shift;
  if (@_) {$self->{FQN} = shift;}
  return $self->{FQN};
}  


sub exists()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $trig  = $names {NAME};
   my $sql   = "select 1
                from $db.dbo.sysobjects o,
                     $db.dbo.sysusers u
                where o.name = '$trig'
                and o.uid = u.uid
                and o.type = 'TR'
                and u.name = '$owner'";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);   
}


sub created()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $trig  = $names {NAME};
   my $sql  = "select crdate
               from $db.dbo.sysobjects o,
                    $db.dbo.sysusers u
               where o.name = '$trig'
                 and o.uid = u.uid
                 and o.type = 'TR'
                 and u.name = '$owner'";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);   
}      



sub text()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $trig  = $names {NAME};
   my $sql = "select text from $db.dbo.syscomments c,
                               $db.dbo.sysobjects o, 
                               $db.dbo.sysusers u
              where c.id = o.id
                and o.type = 'TR'
                and o.uid = u.uid
                and u.name = '$owner'
                and o.name = '$trig'
              order by number, colid";
              
            
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);
}   


sub _FQN_elements()
{
   my $self = shift;
   my ($procFQN) = $self->fq_name();
   my (@pk, $frstDot, $scndDot, $db, $owner, $name, $sql);
   
   $frstDot = index $procFQN, ".", 0;
   if ($frstDot > 0) {
      $scndDot = index $procFQN, ".", $frstDot + 1;
      if ( ! ($scndDot > 0 && $scndDot > $frstDot)) {
         return undef;
      }
   } else {
      return undef;
   }
   
   if ((index $procFQN, ".", $scndDot + 1) > 0) {
      return undef;
   }
   
   $db    = substr $procFQN, 0, $frstDot;
   $owner = substr $procFQN, $frstDot + 1, $scndDot - ($frstDot + 1) ;
   $name  = substr $procFQN, $scndDot + 1;
   my %names = (
                  DB    => $db,
                  OWNER => $owner,
                  NAME  => $name
                );
   return \%names;             
}   



sub _db_query()
{
   my $self = shift;       
	my ($dbh, $sql) = @_;
	my $sth = $dbh->prepare($sql);
	if (ref($sth) eq "DBI::st") {
      $sth->execute();
      my (@result, @row);
	   while (@row=$sth->fetchrow_array) {
         push @result, [ @row ];
	   }	
	   return(\@result);
   } else {
      return undef;
   }
}


1;   