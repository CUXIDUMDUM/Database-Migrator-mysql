package Database::Migrator::mysql;

use strict;
use warnings;

use Database::Migrator::Types qw( Str );
use IPC::Run3 qw( run3 );

use Moose;

with 'Database::Migrator::Core';

has character_set => (
    is        => 'ro',
    isa       => Str,
    predicate => '_has_character_set',
);

has collation => (
    is        => 'ro',
    isa       => Str,
    predicate => '_has_collation',
);

sub _build_database_exists {
    my $self = shift;

    my $databases;
    run3(
        [ $self->_cli_args(), '-e', 'SHOW DATABASES' ],
        \undef,
        \$databases,
        \undef,
    );

    my $database = $self->database();

    return $databases =~ /\Q$database\E/;
}

sub _create_database {
    my $self = shift;

    my $database = $self->database();

    $self->_logger()->info("Creating the $database database");

    my $create_ddl = "CREATE DATABASE $database";
    $create_ddl .= ' CHARACTER SET = ' . $self->character_set()
        if $self->_has_character_set();
    $create_ddl .= ' COLLATE = ' . $self->collation()
        if $self->_has_collation();

    $self->_run_command(
        [ $self->_cli_args(), qw(  --batch -e ), $create_ddl ] );

    my $schema_ddl = read_file( $self->schema_file()->stringify() );

    $self->_run_command(
        [ $self->_cli_args(), '--database', $database, '--batch' ],
        $schema_ddl,
    );
}

sub _cli_args {
    my $self = shift;

    my @cli = 'mysql';
    push @cli, '-u' . $self->user()     if defined $self->user();
    push @cli, '-p' . $self->password() if defined $self->password();
    push @cli, '-h' . $self->host()     if defined $self->host();
    push @cli, '-P' . $self->port()     if defined $self->port();

    return @cli;
}

sub _build_dbh {
    my $self = shift;

    return DBI->connect(
        'dbi:mysql:' . $self->database(),
        $self->user(),
        $self->password(),
        {
            RaiseError         => 1,
            PrintError         => 0,
            PrintWarn          => 1,
            ShowErrorStatement => 1,
        },
    );
}

__PACKAGE__->meta()->make_immutable();

1;
