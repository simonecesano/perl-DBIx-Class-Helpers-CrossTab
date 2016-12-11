package DBIx::Class::Helper::ResultSetColumn::Unique;

sub DBIx::Class::ResultSetColumn::unique {
    my $self = shift;
    return map { $_->[0] } $self->_resultset->search({},
			      {
			       columns => [ $self->{_select} ],
			       group_by => [ $self->{_select} ],
			       distinct => 1
			      } )->cursor->all;
}

1;
