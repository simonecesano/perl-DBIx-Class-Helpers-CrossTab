package DBIx::Class::Helper::ResultSet::CrossTab;

use strict;
use warnings;


use parent 'DBIx::Class::ResultSet';
{
    package DBIx::Class::ResultSet;

    use SQL::Statement;
    use Data::Dump qw/dump/;

    my $p = SQL::Parser->new;

    sub crosstab {
	my $self = shift;
	my $on = shift;
	my @opts = @_;

	my @columns = @{$self->{attrs}->{columns}};

	my @funcs = @opts;
	my @values  = map { { $on => $_ } } $self->get_column($on)->unique;
	# dump \@columns;
	
	my $cross_cols = [ map { my $f = $_; map { $self->ref_to_cross($f, $_) } @values } @funcs ];
	my $as = [ map { $self->ref_to_as($_) } @$cross_cols ];
	my $cross = $self->search({}, {
				       '+select' => $cross_cols,
				       '+as' => $as,
				       group_by => \@columns
				      } );

	# $cross->result_class('DBIx::Class::ResultClass::HashRefInflator');
	return $cross;
    }

    use String::SQLColumnName qw/fix_name/;

    sub ref_to_cross {
	my $self = shift;
	my $function = shift;
	my $value    = shift;
	
	for (ref $function) {
	    /HASH/ && do {
		my ($func, $field) = (%{$function});
		my ($cross, $val)  = (%{$value});
		my $res = sprintf "%s (CASE WHEN %s = %s then %s ELSE NULL END) as %s_%s_%s",
		    $func,
		    $cross,
		    $self->result_source->storage->dbh->quote($val),
		    $field,
		    $func,
		    $field,
		    fix_name($val);
		return \$res;
	    };
	    /SCALAR/ && do {
		my ($cross, $val)  = (%{$value});
		my $s = SQL::Statement->new("select " . $$function, $p);
		my %defs = %{$s->column_defs->[0]};

		$defs{caseargstr} = sprintf "(CASE WHEN %s = %s then %s ELSE NULL END)", $cross, $self->result_source->storage->dbh->quote($val), $defs{argstr};

		$defs{as} = $defs{fullorg} . '_' . fix_name($val);
		for ($defs{as}) {
		    s/^\s|\s$//g;
		    s/\W+/_/g;
		    s/^_|_$//g;
		    s/__/_/g
		}
		my $res = sprintf "%s ( %s %s ) as %s", @defs{qw/name distinct caseargstr as/};
		return \$res;
	    };
	};
    }
    sub ref_to_as {
	my $self = shift;
	my $function = shift;
	for (ref $function) {
	    /HASH/ && do {
		my ($func, $field) = (%{$function});
		return join '_', $func, $field;
	    };
	    /SCALAR/ && do {
		my ($as) = ($$function =~ /\sas\s(.+)/);
		return $as;
	    };
	}
    }
}

1
