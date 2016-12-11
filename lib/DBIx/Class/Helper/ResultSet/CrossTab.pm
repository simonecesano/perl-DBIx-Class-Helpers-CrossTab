package DBIx::Class::Helper::ResultSet::CrossTab;

use strict;
use warnings;

use parent 'DBIx::Class::ResultSet';
{
    package DBIx::Class::ResultSet;

    # use SQL::Statement;
    # my $p = SQL::Parser->new;
    use Data::Dump qw/dump/;


    my $quote = sub { return "'" . $_[0] . "'" };
    
    sub crosstab {
	my $self  = shift;
	my $query = shift;
	my $opts  = shift;

	$quote = sub { return $self->result_source->storage->dbh->quote( $_[0]) };
	
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
		$val = $quote->($val);
		my $res = sprintf "%s (CASE WHEN %s = %s then %s ELSE NULL END) as %s", $func, $cross, $val, $field, $as;
		return \$res;
	    };
	    /SCALAR/ && do {
		my ($cross, $val)  = (%{$value});
		$val = $quote->($val);
		my ($func, $distinct, $arg, $as) = parse_statement($$function);
		my $res = sprintf "%s ( %s (CASE WHEN %s = %s then %s ELSE NULL END) ) as %s", $func, $distinct, $cross, $val, $arg, $as;
		return \$res;
	    };
	};
    }

    sub parse_statement {
	local $_ = shift;
	
	my ($distinct, $as);
	if (s/\((distinct)\s/\(/i) { $distinct = $1 }
	if (s/\((all)\s/\(/i)      { $distinct = $1 }
	
	$distinct ||= 'all';
	
	if (s/\sas\s+(.+)//i) { $as = $1 }
	my ($func, $arg) = ($_ =~ /\s*(\w+)\s*\(\s*(.+?)\s*\)\s*$/i);
	
	my $arg_as = $arg;
	for ($arg_as) { s/\w+?\.//g; s/\W+/_/g;  };
	
	$as ||= join '_', $func, $distinct, $arg_as;
	for ($as) { s/__+/_/g; s/_+$//; }
	
	return $func, $distinct, $arg, $as;
    }

    
    sub ref_to_literal {
	my $self = shift;
	my $function = shift;
	for (ref $function) {
	    /HASH/ && do {
		my ($func, $field) = (%{$function});
		my $as = fix_name(join '_', $func, $field);
		my $res = sprintf "%s (%s) as %s", $func, $field, $as;
		return \$res;
	    };
	    /SCALAR/ && do {
		my ($func, $distinct, $arg, $as) = parse_statement($$function);
		my $res = sprintf "%s ( %s %s ) as %s", $func, $distinct, $arg, $as;
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
