# Role.pm
# By: Carlos Pereyra
# 09/18/01
#
# Functions to get role information (user members)
# Usage:
# Set DBHANDLE (call method dbh with valid database handle)
# Set GROUP    (call method group_name with name of Role)
# 
# ============================================================================================
# Public Methods:
# --------------- 
# get_groupnumber(<optional Role name>)  uses <optional Role name> if provided
#                                        or property GROUP if <optional Role name> not provided
#                                        returns Role groupnum if Role name is the 
#                                        name of a valid database Role.
#                                        returns 0 if group name is not a valid database Role
# --------------------------------------------------------------------------------------------
#
# get_groupusers(<optional Role name>)   uses <optional Role name> if provided
#                                        or property GROUP if <optional Role name> not provided
#                                        Recurses to extract user members from Roles embedded in Roles
#                                        returns a list of Role user members.  
# --------------------------------------------------------------------------------------------
# clear()                                call to this method is required before calling get_groupusers()
#                                        it undefs the global variables that are used to track state
#                                        when get_groupusers recurses.
# --------------------------------------------------------------------------------------------
#
# Private Methods:
#
# _db_query()        returns the result of the SQL statements issued by the public methods.
#
#
 

package Role;
use strict;
use DBI;

my $MAXRECURSIONS = 25;
my (@usermembers, @groupmembers, @groups_done,$safety_counter);


sub new
{
   my $self  = {};
   bless($self);
}


sub dbh()
{
   my $self = shift;
   if (@_) { $self->{DBHANDLE} = shift; }
   return $self->{DBHANDLE};
}   


sub dbname()
{
  my $self = shift;
  if (@_) {$self->{DATABASE} = shift;}
  return $self->{DATABASE};
} 


sub clear() {
   my $self = shift;
   @usermembers    = ();
   @groupmembers   = ();
   @groups_done    = ();
   $safety_counter = 0;
}


sub groupname()
{
  my $self = shift;
  if (@_) {$self->{GROUP} = shift;}
  return $self->{GROUP};
} 


sub get_groupusers()
{
   my $self = shift;
   my $database = $self->dbname();
   my $group;
   my $groupnum;
   if (@_) {
      $group = shift;
   } else {
      $group = $self->groupname();
   }      
   $groupnum = $self->get_groupnumber($group);
    
   if (! $groupnum) {
      return undef;
   } 
   
   my $fqn_members = "$database".'.dbo.sysmembers';
   my $fqn_users = "$database".'.dbo.sysusers';
   my $sql = "select memberuid, name
              from $fqn_members m, $fqn_users u
              where groupuid = $groupnum
              and memberuid = uid";
              
   my $dbh = ${$self->dbh()};
   my @member_info = @{$self->_db_query($dbh, $sql)};
   
   while (1) {
      $safety_counter ++;
      if ($safety_counter > $MAXRECURSIONS) {
         
         return \@usermembers;
      }
      for (my $i = 0; $i <= $#member_info; $i++) {
         if (! $self->get_groupnumber(${@{$member_info[$i]}}[1])){
            my $found = 0;
            for (my $j = 0; $j <= $#usermembers; $j++) {
               if ($usermembers[$j] eq  ${@{$member_info[$i]}}[1]) {
                  $found = 1;
               }
            }
            if (! $found) {   
               push @usermembers,${@{$member_info[$i]}}[1];
            } else {
               $found = 0;
            }
               
         } else {
            my $found = 0;
            for (my $k = 0; $k <= $#groups_done; $k++) {
               if (${@{$member_info[$i]}}[1] eq $groups_done[$k]) {
                  $found = 1;
               }
            }
            if (! $found) {
               push @groups_done , ${@{$member_info[$i]}}[1];          
               push @groupmembers, ${@{$member_info[$i]}}[1];
            } else {
               $found = 0;
            }   
         }       
      }
      if (@groupmembers) {
         my $group = pop @groupmembers;
         # recursive call
         &get_groupusers($self, $group);   
      } else {
         return \@usermembers;
      }
   }   
}
  


sub get_groupnumber()
{
   my $self = shift;
   my $dbh = ${$self->dbh()};
   my $database = $self->dbname();
   my $group;
   if (@_) {
      $group = shift;
   } else {      
      $group = $self->groupname();
   }   
   my $fqn_users = "$database".'.dbo.sysusers';
   my $sql = "select uid, gid 
              from $fqn_users 
              where name = '$group'";
              
   my @groupnums = @{$self->_db_query($dbh, $sql)};           
   # is this a group?  uid and gid must match
   if (${@{$groupnums[0]}}[0] eq ${@{$groupnums[0]}}[1]) {
      return ${@{$groupnums[0]}}[0];
   } else {
      return 0;
   }       
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