use strict;
use warnings;

use Test::More tests => 5;

use lib './t/lib';
use Data::Dump qw/dump/;

use Test::Schema;
use DBIx::Class::Helper::ResultSet::CrossTab;

use Test::DBIx::Class {
    schema_class => 'Test::Schema',
	connect_info => ['dbi:SQLite:dbname=test.db','',''],
	connect_opts => { name_sep => '.', quote_char => '`', },
        fixture_class => '::Populate',
    }, 'Sales';

open STDIN, 't/test_data.txt' || die "no test data";
my @lines = map { [ split /\t/ ] } split /\n/, do { local $/; <STDIN> };

fixtures_ok [ 
	     Sales => [
		       [qw/fruit country channel units/],
		       @lines
		      ],
    ], 'Installed some custom fixtures via the Populate fixture class';


my $f = Test::Schema->connect('dbi:SQLite:test.db') || die;
my $s = $f->resultset('Sales');

my $r = $s->search({}, { columns => [qw/fruit channel/], } )
    ->crosstab({}, { select  => [qw/fruit/], on => [qw/channel/], pivot => [ { sum => 'units' } ], group_by => [qw/fruit/] });
$r->result_class('DBIx::Class::ResultClass::ArrayRefInflator');

is_deeply([ $r->all ],
	  [
	   ["Cherry", 11974, 12258, 15475, 11674],
	   ["Currant", 13720, 13324, 13082, 14498],
	   ["Custard apple", 9222, 13745, 12888, 12862],
	   ["Jackfruit", 13733, 12704, 12861, 12796],
	   ["Orange", 12737, 14390, 14006, 13414],
	   ["Peach", 13142, 15331, 11781, 13827],
	   ["Tomato", 10385, 12721, 11132, 12433],
	  ],
	  'simple crosstab');

$r = $s->search({}, { columns => [qw/fruit channel/], } )->crosstab({ fruit => { -like => 'C%'} },
								       { select  => [qw/fruit/], on => [qw/channel/], pivot => [ { sum => 'units' } ], group_by => [qw/fruit/] }
								      );
$r->result_class('DBIx::Class::ResultClass::ArrayRefInflator');

is_deeply([ $r->all ],
	  [
	   ["Cherry", 11974, 12258, 15475, 11674],
	   ["Currant", 13720, 13324, 13082, 14498],
	   ["Custard apple", 9222, 13745, 12888, 12862],
	  ],
	  'filtered crosstab'
	 );

$r = $s->search({}, { columns => [qw/fruit channel/], } )
    ->crosstab({ channel => { -like => '%w%'} },
	       { select  => [qw/fruit/], on => [qw/channel/], pivot => [ { sum => 'units' } ], group_by => [qw/fruit/] }
	      );
$r->result_class('DBIx::Class::ResultClass::ArrayRefInflator');

is_deeply([ $r->all ],
	  [
	   ["Cherry", 15475, 11674],
	   ["Currant", 13082, 14498],
	   ["Custard apple", 12888, 12862],
	   ["Jackfruit", 12861, 12796],
	   ["Orange", 14006, 13414],
	   ["Peach", 11781, 13827],
	   ["Tomato", 11132, 12433],
	  ],
	  'filtered \'on\' field'
);

$r = $s->search({}, { columns => [qw/fruit channel/], } )->crosstab({ fruit => { -like => 'C%'} },
								    { select  => [qw/fruit/], on => [qw/channel/], pivot => [ \"sum(units)" ], group_by => [qw/fruit/] }
								   );
$r->result_class('DBIx::Class::ResultClass::ArrayRefInflator');
# dump $r->as_query;

is_deeply([ $r->all ],
	  [
	   ["Cherry", 11974, 12258, 15475, 11674],
	   ["Currant", 13720, 13324, 13082, 14498],
	   ["Custard apple", 9222, 13745, 12888, 12862],
	  ],
	  'filtered crosstab'
	 );

# dump [ $r->all ];

# -----------------------------------------------------------------------------
# End of File.
# -----------------------------------------------------------------------------
