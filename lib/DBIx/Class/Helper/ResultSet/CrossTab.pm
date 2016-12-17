package DBIx::Class::Helper::ResultSet::CrossTab;

# ABSTRACT: helper to simulate crosstab

use strict;
use warnings;

use parent 'DBIx::Class::ResultSet';
{
    package DBIx::Class::ResultSet;

    use Data::Dump qw/dump/;

    my $quote = sub { return "'" . $_[0] . "'" };
    
    sub crosstab {
	my $self  = shift;
	my $query = shift;
	my $opts  = shift;

	$quote = sub { return $self->result_source->storage->dbh->quote( $_[0]) };
	my @values;

	if ($opts->{in} && scalar @{$opts->{in}}) {
	    @values = map {
		my %h;
		@h{@{$opts->{on}}} = ref $_ ? @{$_} : ($_);
		\%h;
	    } @{$opts->{in}};
	} else {
	    my $v = $self->search({}, { columns => $opts->{on}, group_by => $opts->{on}, order_by => $opts->{on} });
	    $v->result_class('DBIx::Class::ResultClass::HashRefInflator');
	    @values = map {
		my %h;
		@h{@{$opts->{on}}} = @{$_}{@{$opts->{on}}};
		\%h;
	    } $v->all;
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

	my $when = join ' AND ', map { $_ . '=' . $quote->($value->{$_}) } keys %$value;
	
	for (ref $function) {
	    my $res;
	    /HASH/ && do {
		my ($func, $arg) = (%{$function});
		my $as = fix_name(join '_', $func, $arg, $when);
		my $res = sprintf "%s (CASE WHEN %s then %s ELSE NULL END) as %s",        $func, $when, $arg, $as;
		return \$res;
	    };
	    /SCALAR/ && do {
		my ($func, $distinct, $arg, $as) = parse_statement($$function);
		my $res = sprintf "%s ( %s (CASE WHEN %s then %s ELSE NULL END) ) as %s", $func, $distinct, $when, $arg, $as;
		return \$res;
	    };
	};
    }

    sub parse_statement {
	local $_ = shift;
	
	my ($distinct, $as);

	# get 'distinct' or 'all' in function
	if (s/\((distinct)\s/\(/i) { $distinct = $1 }
	if (s/\((all)\s/\(/i)      { $distinct = $1 }
	$distinct ||= 'all';
	
	# get 'as' part
	if (s/\sas\s+(.+)//i) { $as = $1 }

	# get function (word followed by round bracket)
	# and arguments (the part between the first and last brackets)
	my ($func, $arg) = ($_ =~ /\s*(\w+)\s*\(\s*(.+?)\s*\)\s*$/i);

	# eliminate the me.something from as to make a good
	# string for the as part
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
