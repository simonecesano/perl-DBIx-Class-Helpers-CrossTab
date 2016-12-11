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
	my $self  = shift;
	my $query = shift;
	my $opts  = shift;

	# dump $self->_resolved_attrs->{as};
	my @values;
	if (scalar @{$opts->{on}} == 1) {
	    @values = $opts->{in} ?
		map { { $opts->{on}->[0] => $_} } @{$opts->{in}}
		: map { { $opts->{on}->[0] => $_ } } $self->get_column($opts->{on}->[0])->unique
	} else {
	    die('multiple pivot values not supported yet');
	}

	my @funcs = @{ $opts->{pivot}};

	my @cross_select = map { my $f = $_; map { $self->ref_to_cross($f, $_) } @values } @funcs;
	my @cross_as     = map { $self->ref_to_as($_) } @cross_select;
	
	my $re = quotemeta($opts->{on}->[0]);
	my (@as, @select);
	@select = map { ref $_ ? $self->ref_to_literal($_) : $_ } grep { !/$re/ } @{$self->_resolved_attrs->{select}};
	@as     = map { ref $_ ? $self->ref_to_as($_) : $_ } @select;

	my $cross = $self
	    ->search({}, {
			  'select' => [ @select, @cross_select ],
			  'as'     => [ @as, @cross_as ],
			  group_by => $opts->{group_by}
			 });
	my ($sql, @bind) = @{${$cross->as_query}};
	$sql =~ s/^\s*\((.*)\)\s*$/($1)/;

	my $alias = \[ $sql , @bind ];
	return $self->result_source->resultset->search(undef, {
							       alias => $self->current_source_alias,
							       from => [{
									 $self->current_source_alias => $alias,
									 -alias                      => $self->current_source_alias,
									 -source_handle              => $self->result_source->handle,
									}],
							       result_class => $self->result_class,
							       'as'     => [ @as, @cross_as ],
							       'select' => [ @as, @cross_as ],
							      })
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
		my $as = fix_name(join '_', $func, $field, $val);
		$val = $self->result_source->storage->dbh->quote($val);

		my $res = sprintf "%s (CASE WHEN %s = %s then %s ELSE NULL END) as %s", $func, $cross, $val, $field, $as;
		return \$res;
	    };
	    /SCALAR/ && do {
		my ($cross, $val)  = (%{$value});
		my %defs = %{SQL::Statement->new("select " . $$function, $p)->column_defs->[0]};
		my $as = fix_name($defs{fullorg} . '_' . $val);
		my ($field, $func, $distinct) = @defs{qw/argstr name distinct/};
		$val = $self->result_source->storage->dbh->quote($val);

		my $res = sprintf "%s ( %s (CASE WHEN %s = %s then %s ELSE NULL END) ) as %s", $func, $distinct, $cross, $val, $field, $as;
		return \$res;
	    };
	};
    }

    sub ref_to_literal {
	my $self = shift;
	my $function = shift;
	# dump $function;
	for (ref $function) {
	    /HASH/ && do {
		my ($func, $field) = (%{$function});
		my $as = fix_name(join '_', $func, $field);
		my $res = sprintf "%s (%s) as %s", $func, $field, $as;
		return \$res;
	    };
	    /SCALAR/ && do {
		my %defs = %{SQL::Statement->new("select " . $$function, $p)->column_defs->[0]};
		my $as = fix_name($defs{fullorg});
		my ($field, $func, $distinct) = @defs{qw/argstr name distinct/};
		my $res = sprintf "%s ( %s %s ) as %s", $func, $distinct, $field, $as;
		return \$res;
	    };
	};
    };

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
