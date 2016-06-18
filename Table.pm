# package Table
# By: Carlos Pereyra
# 08/01/01
# Functions to get table information.
# Usage:
# Set DBHANDLE (call sub dbh with valid database handle)
# Set FQN (call sub fq_name with fully qualified name of the table as in database.owner.tablename)
# 
# Public Methods:
# 
# exists()           returns 1 if FQN table exists
# created()          returns create date for table
# rights()           returns list of users or roles and their rights on the table
# rowcount()         returns number of rows in table
# colinfo()          returns table column information
# col_default()      returns default information for column
# col_constraint()   returns constraint information for column
# pk_cols()          returns list of columns in primary key in column order
# idx_cols()         returns list of columns in index in column order
# clustered_idx      returns name of clustered index
# non_clustered_idx  returns name on non clustered index
# identity_col()     returns identity column information
# refers()           returns tablename and columname of a table/column refered to by the table
# refered()          returns tablename and columname of a table/column that reference the table
# triggers()         return trigger information
#
# Private Methods:
#
# _FQN_elements()    returns the individual parts of the fully qualified name
# _db_query()        returns the result of the SQL statements issued by the public methods.
#
#


package Table;
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
   my $table = $names {NAME};
   my $sql = "select 1
              from $db.dbo.sysobjects o,
                   $db.dbo.sysusers u
              where o.name = '$table'
                and o.uid = u.uid
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
   my $table = $names {NAME};
   my $sql = "select crdate
              from $db.dbo.sysobjects o,
                   $db.dbo.sysusers u
              where o.name = '$table'
                and o.uid = u.uid
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
   my $table = $names {NAME};
   my $sql = "select u.name, 
                     case action 
                        when 26  then 'References'
                        when 193 then 'Select'
                        when 195 then 'Insert'
                        when 196 then 'Delete'
                        when 197 then 'Update'
                     end
              from $db.dbo.sysprotects p,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysusers u,
                   $db.dbo.sysusers u2
              where p.id = o.id
                and o.type = 'U'
                and p.uid = u.uid
                and o.name = '$table'
                and o.uid = u2.uid $where_plus $orderby";
                
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);

}


sub rowcount()
{
   my $self = shift;
   my ($tableFQN) = $self->fq_name();
   my $sql = "select count(*)
              from $tableFQN";
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
   my $table = $names {NAME};   
   $sql = "select c.name,
                  type_name(xusertype),  
 	               convert(int, length),  
                  xprec,                 
                  xscale,                
                  case
                  when isnullable = 0
                     then 'no'
                     else 'yes'
                  end             
           from $db.dbo.syscolumns c,
                $db.dbo.sysobjects o,
                $db.dbo.sysusers u
           where c.id = o.id
             and o.type = 'U'
             and o.uid = u.uid
             and u.name = '$owner'
             and o.name = '$table' $where_plus $orderby";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql); 
 
}


sub col_default()
{
   my $self = shift;
   my ($colname) = @_;
   my ($sql);
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};   
   $sql = "select text
           from $db.dbo.syscomments c,
                $db.dbo.sysobjects o,
                $db.dbo.sysobjects o1,
                $db.dbo.sysconstraints t,
                $db.dbo.syscolumns l,
                $db.dbo.sysusers u
           where t.constid = o.id
             and o.type = 'D'
             and l.name = '$colname'
             and l.id = o1.id
             and t.colid = l.colid
             and o.parent_obj = o1.id
             and c.id = o.id
             and o1.name = '$table'
             and o1.type = 'U'
             and u.uid = o.uid
             and u.name = '$owner'";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);           
}


sub col_constraint()
{
   my $self = shift;
   my ($colname) = @_;
   my ($sql);
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};   
   $sql = "select text
           from $db.dbo.syscomments c,
                $db.dbo.sysobjects o,
                $db.dbo.sysobjects o1,
                $db.dbo.sysconstraints t,
                $db.dbo.sysusers u
           where t.constid = o.id
             and o.type = 'C'
             and o.parent_obj = o1.id
             and o1.name = '$table'
             and o1.type = 'U'
             and c.id = o.id
             and u.uid = o.uid
             and u.name = '$owner'
             and substring(text, charindex('[', text) + 1,
                                 charindex(']', text) - (charindex('[', text) + 1)) = '$colname'";
            
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);           
}

	   
sub pk_cols()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql;   
   $sql = "select indid
           from $db.dbo.sysindexes 
           where name = (select name
                         from $db.dbo.sysobjects
                         where xtype = 'PK'
                           and parent_obj = (select o.id
                                             from $db.dbo.sysobjects o,
                                                  $db.dbo.sysusers u
                                             where o.name = '"."$table"."'
                                               and o.type = 'U'
                                               and o.uid = u.uid 
                                               and u.name = '$owner'))
             and id = (select o.id
                       from $db.dbo.sysobjects o,
                            $db.dbo.sysusers u
                       where o.name = '$table'
                         and o.type = 'U'
                         and o.uid = u.uid
                         and u.name = '$owner')";
                       
   my ($dbh, @rows, @pk);
   $dbh = ${$self->dbh()};
   @rows = @{$self->_db_query($dbh, $sql)};
   if (@rows) {
      my $tableFQN = $self->fq_name();
      my $colCtr = 1; 
      while ($colCtr <= 16) { # 16 is max columns in index
         $sql = "select index_col('$tableFQN', @{$rows[0]}, $colCtr)";
         my $pk = $self->_db_query($dbh, $sql);
         # the following dereferencing is needed to test the actual value against empty
         # empty means we have gotten beyond the end of the pk columns
         my @test_val = @{${@{$pk}}[0]};
         last if $test_val[0] eq "";
         # $colCtr -1 because we want to start the @pk array at element 0
         $pk[$colCtr -1] = \@test_val;
         $colCtr++;
      }
         return (\@pk);
    } 
   return undef; # no primary key defined   
}   


sub idx_cols()
{
   my $self = shift;
   my ($idx_name) = @_;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my ($sql, $dbh);
   $dbh = ${$self->dbh()};
   my $tableFQN = $self->fq_name();
   $sql = "select indid
           from $db.dbo.sysindexes 
           where name = '$idx_name'
             and indid > 0 
             and indid < 255 
             and id = (select o.id
                       from $db.dbo.sysobjects o,
                            $db.dbo.sysusers u
                       where o.name = '$table'
                         and o.type = 'U'
                         and o.uid = u.uid
                         and u.name = '$owner')";
                         
                 
                         
   my (@rows, @key);
   $dbh = ${$self->dbh()};
   @rows = @{$self->_db_query($dbh, $sql)};
   my $colCtr = 1; 
   while ($colCtr <= 16) { # 16 is max columns in index
      $sql = "select index_col('$tableFQN', @{$rows[0]}, $colCtr)";
      my $key = $self->_db_query($dbh, $sql);
      # the following dereferencing is needed to test the actual value against empty
      # empty means we have gotten beyond the end of the key columns
      my @test_val = @{${@{$key}}[0]};
      last if $test_val[0] eq "";
      # $colCtr -1 because we want to start the @key array at element 0
      $key[$colCtr -1] = \@test_val;
      $colCtr++;
   }
   return (\@key);
}   


sub clustered_idx ()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select i.name
              from $db.dbo.sysindexes i,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysusers u
              where i.id = o.id
                and o.uid = u.uid
                and o.type = 'U'
                and o.name = '$table'
                and u.name = '$owner'
                and i.indid = 1"; 
                
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);
   
}


sub nonclustered_idx ()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select i.name
              from $db.dbo.sysindexes i,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysusers u
              where i.id = o.id
                and o.uid = u.uid
                and o.type = 'U'
                and o.name = '$table'
                and u.name = '$owner'
                and substring(i.name, 1, 1) <> '_'
                and i.indid > 1 and i.indid < 255"; 
                
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);
   
}


sub identity_col()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select s.name
              from $db.dbo.syscolumns s,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysusers u
              where s.id = o.id
                and o.type = 'U'
                and o.name = '$table'
                and o.uid = u.uid
                and u.name = '$owner'
                and s.autoval is not null";
   my $dbh = ${$self->dbh()};
   my $col = $self->_db_query($dbh, $sql);
   if (defined @{${@{$col}}[0]}) {;
      my @test_val = @{${@{$col}}[0]};
      return $self->colinfo($test_val[0]);
   }
   else {
      return undef;
   }   
      
}    


sub refers()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select o2.name, c.name
              from $db.dbo.syscolumns c,
                   $db.dbo.sysforeignkeys fk,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysobjects o2,
                   $db.dbo.sysusers u
              where c.id = fkeyid 
                and colorder = fkey
                and c.id = o.id
                and o2.id = fk.rkeyid
                and o.type = 'U'
                and o.name = '$table'
                and o.uid = u.uid
                and u.name = '$owner'
              order by keyno";
  my $dbh = ${$self->dbh()};
  return $self->_db_query($dbh, $sql);   
}


sub refered()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select o2.name, c.name
              from $db.dbo.syscolumns c,
                   $db.dbo.sysforeignkeys fk,
                   $db.dbo.sysobjects o,
                   $db.dbo.sysobjects o2,
                   $db.dbo.sysusers u
              where c.id = rkeyid 
                and colorder = rkey
                and c.id = o.id
                and o2.id = fk.fkeyid
                and o.type = 'U'
                and o.name = '$table'
                and o.uid = u.uid
                and u.name = '$owner'
              order by keyno";

  my $dbh = ${$self->dbh()};
  return $self->_db_query($dbh, $sql);   
}


sub triggers()
{
   my $self = shift;
   my %names = %{$self->_FQN_elements()};
   my $db    = $names {DB};
   my $owner = $names {OWNER};
   my $table = $names {NAME};
   my $sql = "select o.name,
               case o.status &2048
                  when 0 then 'Enabled'
                  else  'Disabled'
               end as State,
               case o.status &1792
                  when 256 then 'D'
                  when 512 then 'U'
                  when 768 then 'DU'
                  when 1024 then 'I'
                  when 1280 then 'DI'
                  when 1536 then 'UI'
               end as Type
              from $db.dbo.sysobjects o, $db.dbo.sysobjects o1, $db.dbo.sysusers u
              where o.type = 'TR'
                and o.parent_obj = o1.id
                and o1.name = '$table'
                and o1.type = 'U'
                and o1.uid = u.uid
                and u.name = '$owner'";
   my $dbh = ${$self->dbh()};
   return $self->_db_query($dbh, $sql);
}


sub _FQN_elements()
{
   my $self = shift;
   my ($tableFQN) = $self->fq_name();
   my (@pk, $frstDot, $scndDot, $db, $owner, $name, $sql);
   
   $frstDot = index $tableFQN, ".", 0;
   if ($frstDot > 0) {
      $scndDot = index $tableFQN, ".", $frstDot + 1;
      if ( ! ($scndDot > 0 && $scndDot > $frstDot)) {
         return undef;
      }
   } else {
      return undef;
   }
   
   if ((index $tableFQN, ".", $scndDot + 1) > 0) {
      return undef;
   }
   
   $db    = substr $tableFQN, 0, $frstDot;
   $owner = substr $tableFQN, $frstDot + 1, $scndDot - ($frstDot + 1) ;
   $name  = substr $tableFQN, $scndDot + 1;
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