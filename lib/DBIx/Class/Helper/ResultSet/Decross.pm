package DBIx::Class::Helper::ResultSet::Decross;

{
    package DBIx::Class::ResultSet;

    use strict;
    use Data::Dump qw/dump/;
    
    sub decross {
	my $self = shift;
	my $re   = shift;
	
	my @cols = $self->result_source->columns;
	my @as   = @{$self->_resolved_attrs->{as}};
	
	for (ref $re) {
	    /REGEXP/i && do {
		@cols = grep { /$re/ } @cols;
		last;
	    };
	    /ARRAY/ && do {
		@cols = @{$re};
		last;
	    };
	    /CODE/ && do {
		@cols = grep { $re->($_) } @cols;
		last;
	    };
	    $re = quotemeta($re);
	    @cols = grep { /$re/ } @cols;
	}
	my (@sql, @params);

	for (@cols) {
	    my $s = $self->search({}, { '+select' => [ \"\'$_\' as field_name", \"$_ as field_value"  ] });
	    my ($sql, @bind) = @{${$s->as_query}};
	    $sql =~ s/^\s*\((.*)\)\s*$/$1/;
	    
	    push @sql, $sql;
	    push @params, @bind;
	}
	
	my $query = q<(> . join(" union all ", @sql). q<)>;

	return $self->result_source->resultset->search(undef, {
							       alias => $self->current_source_alias,
							       from => [{
									 $self->current_source_alias => \[ $query, @params ],
									-alias                      => $self->current_source_alias,
									-source_handle              => $self->result_source->handle,
								    }],
						       'columns' => [@as, qw/field_name field_value/],
						       result_class => $self->result_class,
						   });
    }
}

1
