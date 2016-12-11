package DBIx::Class::ResultClass::ArrayRefInflator;
use Data::Dump qw/dump/;

sub inflate_result {
    return \@{$_[2]}{$_[0]->_resolved_attrs->{as}}
}

sub header {
    return shift->_resolved_attrs->{as}
}

1;
