# package Procedure
# By: Carlos Pereyra
# 08/01/01
# Functions to get proc information.
# Usage:
# Set DBHANDLE (call sub dbh with valid database handle)
# Set FQN (call sub fq_name with fully qualified name of the proc as in database.owner.procname)
# 
# Public Methods:
# 
# exists()           returns 1 if FQN procedure exists
# created()          returns create date for proc
# rights()           returns list of users or roles and their rights on the procedure
# colinfo()          returns procedure column information
# text()             returns the procedure code
#
# Private Methods:
#
# _FQN_elements()    returns the individual parts of the fully qualified name
# _db_query()        returns the result of the SQL statements issued by the public methods.
#
#


package Procedure;
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
   my $proc  = $names {NAME};
   my $sql   = "select 1
                from $db.dbo.sysobjects o,
                     $db.dbo.sysusers u
                where o.name = '$proc'
                and o.uid = u.uid
                and o.type = 'P'
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
   my $proc  = $names {NAME};
   my $sql  = "select crdate
               from $db.dbo.sysobjects o,
                    $db.dbo.sysusers u
               where o.name = '$proc'
                 and o.uid = u.uid
                 and o.type = 'P'
                 and u.name = '$owner'";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);   
}      


sub rights()
{
   my $self = shift;
   my ($where_plus, $orderby);
   if (@_) { # just one user
      $where_plus = shift;
      $where_plus = " and u.name = '$where_plus'";
      $orderby = "order by 2";
      
   } else {
      $orderby = " order by 1, 2"; 
      $where_plus = "";
   }
      
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $proc  = $names {NAME};
   my $sql   = "select u.name, 
                  case action 
                     when 224 then 'Execute'
                  end
                from $db.dbo.sysprotects p,
                     $db.dbo.sysobjects o,
                     $db.dbo.sysusers u,
                     $db.dbo.sysusers u2
                where p.id = o.id
                  and o.type = 'P'
                  and p.uid = u.uid
                  and o.name = '$proc'
                  and o.uid = u2.uid $where_plus $orderby";
                
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);

}


sub colinfo()
{
   my $self = shift;
   my ($sql, $where_plus, $orderby);
   if (@_) { # just one column
      $where_plus = shift;
      $where_plus = " and c.name = '$where_plus'";
      $orderby = "";
   } else {
      $orderby = " order by colorder"; 
      $where_plus = "";
   }
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $proc  = $names {NAME};   
   $sql = "select c.name,
                  type_name(xusertype),  
 	               convert(int, length),  
                  xprec,                 
                  xscale 
           from $db.dbo.syscolumns c,
                $db.dbo.sysobjects o,
                $db.dbo.sysusers u
           where c.id = o.id
             and o.type = 'P'
             and o.uid = u.uid
             and u.name = '$owner'
             and o.name = '$proc' $where_plus $orderby";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql); 
 
}


sub text()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $proc  = $names {NAME};
   my $sql = "select text from $db.dbo.syscomments c,
                               $db.dbo.sysobjects o, 
                               $db.dbo.sysusers u
              where c.id = o.id
                and o.type = 'P'
                and o.uid = u.uid
                and u.name = '$owner'
                and o.name = '$proc'
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