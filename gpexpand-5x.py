#!/usr/bin/env python
# Line too long            - pylint: disable=C0301
# Invalid name             - pylint: disable=C0103
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
from gppylib.mainUtils import getProgramName

import copy
import datetime
import os
import sys
import socket
import signal
import traceback
from time import strftime, sleep

try:
    from gppylib.commands.unix import *
    from gppylib.fault_injection import inject_fault
    from gppylib.commands.gp import *
    from gppylib.commands.pg import PgControlData
    from gppylib.gparray import GpArray, MODE_CHANGELOGGING, STATUS_DOWN
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
    from gppylib.db import catalog
    from gppylib.db import dbconn
    from gppylib.userinput import *
    from gppylib.operations.startSegments import MIRROR_MODE_MIRRORLESS
    from gppylib.system import configurationInterface, configurationImplGpdb
    from gppylib.system.environment import GpMasterEnvironment
    from pygresql.pgdb import DatabaseError
    from pygresql import pg
    from gppylib.gpcatalog import MASTER_ONLY_TABLES
    from gppylib.operations.package import SyncPackages
    from gppylib.operations.utils import ParallelOperation
    from gppylib.parseutils import line_reader, parse_gpexpand_segment_line, \
        canonicalize_address
    from gppylib.operations.filespace import PG_SYSTEM_FILESPACE, GP_TRANSACTION_FILES_FILESPACE, \
        GP_TEMPORARY_FILES_FILESPACE, GetCurrentFilespaceEntries, GetFilespaceEntries, GetFilespaceEntriesDict, \
        RollBackFilespaceChanges, GetMoveOperationList, FileType, UpdateFlatFiles
    from gppylib.heapchecksum import HeapChecksum

except ImportError, e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

# constants
MAX_PARALLEL_EXPANDS = 96
MAX_BATCH_SIZE = 128

GPDB_STOPPED = 1
GPDB_STARTED = 2
GPDB_UTILITY = 3

FILE_SPACES_INPUT_FILENAME_SUFFIX = ".fs"
SEGMENT_CONFIGURATION_BACKUP_FILE = "gpexpand.gp_segment_configuration"
FILE_SPACES_INPUT_FILE_LINE_1_PREFIX = "filespaceOrder"

#global var
_gp_expand = None

description = ("""
Adds additional segments to a pre-existing GPDB Array.
""")

_help = ["""
The input file should be be a plain text file with a line for each segment
to add with the format:

  <hostname>:<port>:<data_directory>:<dbid>:<content>:<definedprimary>
""",
         """
         If an input file is not specified, gpexpand will ask a series of questions
         and create one.
         """,
         ]

_TODO = ["""

Remaining TODO items:
====================
""",

         """* smarter heuristics on setting ranks. """,

         """* make sure system isn't in "readonly mode" during setup. """,

         """* need a startup validation where we check the status detail
             with gp_distribution_policy and make sure that our book
             keeping matches reality. we don't have a perfect transactional
             model since the tables can be in a different database from
             where the gpexpand schema is kept. """,

         """* currently requires that GPHOME and PYTHONPATH be set on all of the remote hosts of
              the system.  should get rid of this requirement. """
         ]

_usage = """[-f hosts_file] [-D database_name]

gpexpand -i input_file [-D database_name] [-B batch_size] [-V] [-t segment_tar_dir] [-S]

gpexpand [-d duration[hh][:mm[:ss]] | [-e 'YYYY-MM-DD hh:mm:ss']]
         [-a] [-n parallel_processes] [-D database_name]

gpexpand -r [-D database_name]

gpexpand -c [-D database_name]

gpexpand -? | -h | --help | --verbose | -v"""

EXECNAME = os.path.split(__file__)[-1]


# ----------------------- Command line option parser ----------------------

def parseargs():
    parser = OptParser(option_class=OptChecker,
                       description=' '.join(description.split()),
                       version='%prog version $Revision$')
    parser.setHelp(_help)
    parser.set_usage('%prog ' + _usage)
    parser.remove_option('-h')

    parser.add_option('-c', '--clean', action='store_true',
                      help='remove the expansion schema.')
    parser.add_option('-r', '--rollback', action='store_true',
                      help='rollback failed expansion setup.')
    parser.add_option('-V', '--novacuum', action='store_true',
                      help='Do not vacuum catalog tables before creating schema copy.')
    parser.add_option('-a', '--analyze', action='store_true',
                      help='Analyze the expanded table after redistribution.')
    parser.add_option('-d', '--duration', type='duration', metavar='[h][:m[:s]]',
                      help='duration from beginning to end.')
    parser.add_option('-e', '--end', type='datetime', metavar='datetime',
                      help="ending date and time in the format 'YYYY-MM-DD hh:mm:ss'.")
    parser.add_option('-i', '--input', dest="filename",
                      help="input expansion configuration file.", metavar="FILE")
    parser.add_option('-f', '--hosts-file', metavar='<hosts_file>',
                      help='file containing new host names used to generate input file')
    parser.add_option('-D', '--database', dest='database',
                      help='Database to create the gpexpand schema and tables in.  If this ' \
                           'option is not given, PGDATABASE will be used.  The template1, ' \
                           'template0 and postgres databases cannot be used.')
    parser.add_option('-B', '--batch-size', type='int', default=16, metavar="<batch_size>",
                      help='Expansion configuration batch size. Valid values are 1-%d' % MAX_BATCH_SIZE)
    parser.add_option('-n', '--parallel', type="int", default=1, metavar="<parallel_processes>",
                      help='number of tables to expand at a time. Valid values are 1-%d.' % MAX_PARALLEL_EXPANDS)
    parser.add_option('-v', '--verbose', action='store_true',
                      help='debug output.')
    parser.add_option('-S', '--simple-progress', action='store_true',
                      help='show simple progress.')
    parser.add_option('-t', '--tardir', default='.', metavar="FILE",
                      help='Tar file directory.')
    parser.add_option('-h', '-?', '--help', action='help',
                      help='show this help message and exit.')
    parser.add_option('-s', '--silent', action='store_true',
                      help='Do not prompt for confirmation to proceed on warnings')
    parser.add_option('--usage', action="briefhelp")

    parser.set_defaults(verbose=False, filters=[], slice=(None, None))

    # Parse the command line arguments
    (options, args) = parser.parse_args()
    return options, args, parser

def validate_options(options, args, parser):
    if len(args) > 0:
        logger.error('Unknown argument %s' % args[0])
        parser.exit()

    # -n sanity check
    if options.parallel > MAX_PARALLEL_EXPANDS or options.parallel < 1:
        logger.error('Invalid argument.  parallel value must be >= 1 and <= %d' % MAX_PARALLEL_EXPANDS)
        parser.print_help()
        parser.exit()

    proccount = os.environ.get('GP_MGMT_PROCESS_COUNT')
    if options.batch_size == 16 and proccount is not None:
        options.batch_size = int(proccount)

    if options.batch_size < 1 or options.batch_size > 128:
        logger.error('Invalid argument.  -B value must be >= 1 and <= %s' % MAX_BATCH_SIZE)
        parser.print_help()
        parser.exit()

    # OptParse can return date instead of datetime so we might need to convert
    if options.end and not isinstance(options.end, datetime.datetime):
        options.end = datetime.datetime.combine(options.end, datetime.time(0))

    if options.end and options.end < datetime.datetime.now():
        logger.error('End time occurs in the past')
        parser.print_help()
        parser.exit()

    if options.end and options.duration:
        logger.warn('Both end and duration options were given.')
        # Both a duration and an end time were given.
        if options.end > datetime.datetime.now() + options.duration:
            logger.warn('The duration argument will be used for the expansion end time.')
            options.end = datetime.datetime.now() + options.duration
        else:
            logger.warn('The end argument will be used for the expansion end time.')
    elif options.duration:
        options.end = datetime.datetime.now() + options.duration

    # -c and -r options are mutually exclusive
    if options.rollback and options.clean:
        rollbackOpt = "--rollback" if "--rollback" in sys.argv else "-r"
        cleanOpt = "--clean" if "--clean" in sys.argv else "-c"
        logger.error("%s and %s options cannot be specified together." % (rollbackOpt, cleanOpt))
        parser.exit()

    try:
        options.master_data_directory = get_masterdatadir()
        options.gphome = get_gphome()
    except GpError, msg:
        logger.error(msg)
        parser.exit()

    if not os.path.exists(options.master_data_directory):
        logger.error('Master data directory does not exist.')
        parser.exit()

    if options.database and (options.database.lower() == 'template0'
                             or options.database.lower() == 'template1'
                             or options.database.lower() == 'postgres'):
        logger.error('%s cannot be used to store the gpexpand schema and tables' % options.database)
        parser.exit()
    elif not options.database:
        options.database = os.getenv('PGDATABASE')

    options.pgport = int(os.getenv('PGPORT', 5432))

    return options, args


# -------------------------------------------------------------------------
# process information functions
def create_pid_file(master_data_directory):
    """Creates gpexpand pid file"""
    try:
        fp = open(master_data_directory + '/gpexpand.pid', 'w')
        fp.write(str(os.getpid()))
    except IOError:
        raise
    finally:
        if fp: fp.close()


def remove_pid_file(master_data_directory):
    """Removes gpexpand pid file"""
    try:
        os.unlink(master_data_directory + '/gpexpand.pid')
    except:
        pass


def is_gpexpand_running(master_data_directory):
    """Checks if there is another instance of gpexpand running"""
    is_running = False
    try:
        fp = open(master_data_directory + '/gpexpand.pid', 'r')
        pid = int(fp.readline().strip())
        fp.close()
        is_running = check_pid(pid)
    except IOError:
        pass
    except Exception:
        raise

    return is_running


def gpexpand_status_file_exists(master_data_directory):
    """Checks if gpexpand.pid exists"""
    return os.path.exists(master_data_directory + '/gpexpand.status')


# -------------------------------------------------------------------------
# expansion schema

undone_status = "NOT STARTED"
start_status = "IN PROGRESS"
done_status = "COMPLETED"
does_not_exist_status = 'NO LONGER EXISTS'

gpexpand_schema = 'gpexpand'
create_schema_sql = "CREATE SCHEMA " + gpexpand_schema
drop_schema_sql = "DROP schema IF EXISTS %s CASCADE" % gpexpand_schema

status_table = 'status'
status_table_sql = """CREATE TABLE %s.%s
                        ( status text,
                          updated timestamp ) """ % (gpexpand_schema, status_table)

status_detail_table = 'status_detail'
status_detail_table_sql = """CREATE TABLE %s.%s
                        ( dbname text,
                          fq_name text,
                          schema_oid oid,
                          table_oid oid,
                          distribution_policy smallint[],
                          distribution_policy_names text,
                          distribution_policy_coloids text,
                          storage_options text,
                          rank int,
                          status text,
                          expansion_started timestamp,
                          expansion_finished timestamp,
                          source_bytes numeric ) """ % (gpexpand_schema, status_detail_table)
# gpexpand views
progress_view = 'expansion_progress'
progress_view_simple_sql = """CREATE VIEW %s.%s AS
SELECT
    CASE status
        WHEN '%s' THEN 'Tables Expanded'
        WHEN '%s' THEN 'Tables Left'
    END AS Name,
    count(*)::text AS Value
FROM %s.%s GROUP BY status""" % (gpexpand_schema, progress_view,
                                 done_status, undone_status, gpexpand_schema, status_detail_table)

progress_view_sql = """CREATE VIEW %s.%s AS
SELECT
    CASE status
        WHEN '%s' THEN 'Tables Expanded'
        WHEN '%s' THEN 'Tables Left'
        WHEN '%s' THEN 'Tables In Progress'
    END AS Name,
    count(*)::text AS Value
FROM %s.%s GROUP BY status

UNION

SELECT
    CASE status
        WHEN '%s' THEN 'Bytes Done'
        WHEN '%s' THEN 'Bytes Left'
        WHEN '%s' THEN 'Bytes In Progress'
    END AS Name,
    SUM(source_bytes)::text AS Value
FROM %s.%s GROUP BY status

UNION

SELECT
    'Estimated Expansion Rate' AS Name,
    (SUM(source_bytes) / (1 + extract(epoch FROM (max(expansion_finished) - min(expansion_started)))) / 1024 / 1024)::text || ' MB/s' AS Value
FROM %s.%s
WHERE status = '%s'
AND
expansion_started > (SELECT updated FROM %s.%s WHERE status = '%s' ORDER BY updated DESC LIMIT 1)

UNION

SELECT
'Estimated Time to Completion' AS Name,
CAST((SUM(source_bytes) / (
SELECT 1 + SUM(source_bytes) / (1 + (extract(epoch FROM (max(expansion_finished) - min(expansion_started)))))
FROM %s.%s
WHERE status = '%s'
AND
expansion_started > (SELECT updated FROM %s.%s WHERE status = '%s' ORDER BY
updated DESC LIMIT 1)))::text || ' seconds' as interval)::text AS Value
FROM %s.%s
WHERE status = '%s'
  OR status = '%s'""" % (gpexpand_schema, progress_view,
                         done_status, undone_status, start_status,
                         gpexpand_schema, status_detail_table,
                         done_status, undone_status, start_status,
                         gpexpand_schema, status_detail_table,
                         gpexpand_schema, status_detail_table,
                         done_status,
                         gpexpand_schema, status_table,
                         'EXPANSION STARTED',
                         gpexpand_schema, status_detail_table,
                         done_status,
                         gpexpand_schema, status_table,
                         'EXPANSION STARTED',
                         gpexpand_schema, status_detail_table,
                         start_status, undone_status)

unalterable_table_sql = """
SELECT
    current_database() AS database,
    pg_catalog.quote_ident(nspname) || '.' ||
    pg_catalog.quote_ident(relname) AS table,
    attnum,
    attlen,
    attbyval,
    attstorage,
    attalign,
    atttypmod,
    attndims,
    reltoastrelid != 0 AS istoasted
FROM
    pg_catalog.pg_attribute,
    pg_catalog.pg_class,
    pg_catalog.pg_namespace
WHERE
    attisdropped
    AND attnum >= 0
    AND attrelid = pg_catalog.pg_class.oid
    AND relnamespace = pg_catalog.pg_namespace.oid
    AND (attlen, attbyval, attalign, attstorage) NOT IN
        (SELECT typlen, typbyval, typalign, typstorage
        FROM pg_catalog.pg_type
        WHERE typisdefined AND typtype='b' )
ORDER BY
    attrelid, attnum
"""

has_unique_index_sql = """
SELECT
    current_database() || '.' || pg_catalog.quote_ident(nspname) || '.' || pg_catalog.quote_ident(relname) AS table
FROM
    pg_class c,
    pg_namespace n,
    pg_index i
WHERE
  i.indrelid = c.oid
  AND c.relnamespace = n.oid
  AND i.indisunique
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast',
                        'pg_bitmapindex', 'pg_aoseg')
"""


# -------------------------------------------------------------------------
class InvalidStatusError(Exception): pass


class ValidationError(Exception): pass


# -------------------------------------------------------------------------
class GpExpandStatus():
    """Class that manages gpexpand status file.

    The status file is placed in the master data directory on both the master and
    the standby master.  it's used to keep track of where we are in the progression.
    """

    def __init__(self, logger, master_data_directory, master_mirror=None):
        self.logger = logger

        self._status_values = {'UNINITIALIZED': 1,
                               'EXPANSION_PREPARE_STARTED': 2,
                               'BUILD_SEGMENT_TEMPLATE_STARTED': 3,
                               'BUILD_SEGMENT_TEMPLATE_DONE': 4,
                               'BUILD_SEGMENTS_STARTED': 5,
                               'BUILD_SEGMENTS_DONE': 6,
                               'UPDATE_OLD_SEGMENTS_STARTED': 7,
                               'UPDATE_OLD_SEGMENTS_DONE': 8,
                               'UPDATE_CATALOG_STARTED': 9,
                               'UPDATE_CATALOG_DONE': 10,
                               'SETUP_EXPANSION_SCHEMA_STARTED': 11,
                               'SETUP_EXPANSION_SCHEMA_DONE': 12,
                               'PREPARE_EXPANSION_SCHEMA_STARTED': 13,
                               'PREPARE_EXPANSION_SCHEMA_DONE': 14,
                               'EXPANSION_PREPARE_DONE': 15
                               }
        self._status = []
        self._status_info = []
        self._master_data_directory = master_data_directory
        self._master_mirror = master_mirror
        self._status_filename = master_data_directory + '/gpexpand.status'
        self._status_standby_filename = master_data_directory + '/gpexpand.standby.status'
        self._fp = None
        self._fp_standby = None
        self._temp_dir = None
        self._input_filename = None
        self._original_primary_count = None
        self._gp_segment_configuration_backup = None

        if os.path.exists(self._status_filename):
            self._read_status_file()

    def _read_status_file(self):
        """Reads in an existing gpexpand status file"""
        self.logger.debug("Trying to read in a pre-existing gpexpand status file")
        try:
            self._fp = open(self._status_filename, 'a+')
            self._fp.seek(0)

            for line in self._fp:
                (status, status_info) = line.rstrip().split(':')
                if status == 'BUILD_SEGMENT_TEMPLATE_STARTED':
                    self._temp_dir = status_info
                elif status == 'BUILD_SEGMENTS_STARTED':
                    self._seg_tarfile = status_info
                elif status == 'BUILD_SEGMENTS_DONE':
                    self._number_new_segments = status_info
                elif status == 'EXPANSION_PREPARE_STARTED':
                    self._input_filename = status_info
                elif status == 'UPDATE_OLD_SEGMENTS_STARTED':
                    self._original_primary_count = int(status_info)
                elif status == 'UPDATE_CATALOG_STARTED':
                    self._gp_segment_configuration_backup = status_info

                self._status.append(status)
                self._status_info.append(status_info)
        except IOError:
            raise

        if not self._status_values.has_key(self._status[-1]):
            raise InvalidStatusError('Invalid status file.  Unknown status %s' % self._status)

    def create_status_file(self):
        """Creates a new gpexpand status file"""
        try:
            self._fp = open(self._status_filename, 'w')
            if self._master_mirror:
                self._fp_standby = open(self._status_standby_filename, 'w')
                self._fp_standby.write('UNINITIALIZED:None\n')
                self._fp_standby.flush()
            self._fp.write('UNINITIALIZED:None\n')
            self._fp.flush()
            self._status.append('UNINITIALIZED')
            self._status_info.append('None')
        except IOError:
            raise

        self._sync_status_file()

    def _sync_status_file(self):
        """Syncs the gpexpand status file with the master mirror"""
        if self._master_mirror:
            cpCmd = RemoteCopy('gpexpand copying status file to master mirror',
                               self._status_standby_filename, self._master_mirror.getSegmentHostName(),
                               self._status_filename)
            cpCmd.run(validateAfter=True)

    def set_status(self, status, status_info=None):
        """Sets the current status.  gpexpand status must be set in
           proper order.  Any out of order status result in an
           InvalidStatusError exception"""
        self.logger.debug("Transitioning from %s to %s" % (self._status[-1], status))

        if not self._fp:
            raise InvalidStatusError('The status file is invalid and cannot be written to')
        if not self._status_values.has_key(status):
            raise InvalidStatusError('%s is an invalid gpexpand status' % status)
        # Only allow state transitions forward or backward 1
        if self._status and \
                        self._status_values[status] != self._status_values[self._status[-1]] + 1:
            raise InvalidStatusError('Invalid status transition from %s to %s' % (self._status[-1], status))
        if self._master_mirror:
            self._fp_standby.write('%s:%s\n' % (status, status_info))
            self._fp_standby.flush()
            self._sync_status_file()
        self._fp.write('%s:%s\n' % (status, status_info))
        self._fp.flush()
        self._status.append(status)
        self._status_info.append(status_info)

    def get_current_status(self):
        """Gets the current status that has been written to the gpexpand
           status file"""
        if (len(self._status) > 0 and len(self._status_info) > 0):
            return (self._status[-1], self._status_info[-1])
        else:
            return (None, None)

    def get_status_history(self):
        """Gets the full status history"""
        return zip(self._status, self._status_info)

    def remove_status_file(self):
        """Closes and removes the gpexand status file"""
        if self._fp:
            self._fp.close()
            self._fp = None
        if self._fp_standby:
            self._fp_standby.close()
            self._fp_standby = None
        if os.path.exists(self._status_filename):
            os.unlink(self._status_filename)
        if os.path.exists(self._status_standby_filename):
            os.unlink(self._status_standby_filename)
        if self._master_mirror:
            RemoveFile.remote('gpexpand master mirror status file cleanup',
                              self._master_mirror.getSegmentHostName(),
                              self._status_filename)

    def remove_segment_configuration_backup_file(self):
        """ Remove the segment configuration backup file """
        self.logger.debug("Removing segment configuration backup file")
        if self._gp_segment_configuration_backup != None and os.path.exists(
                self._gp_segment_configuration_backup) == True:
            os.unlink(self._gp_segment_configuration_backup)

    def get_temp_dir(self):
        """Gets temp dir that was used during template creation"""
        return self._temp_dir

    def get_input_filename(self):
        """Gets input file that was used by expansion setup"""
        return self._input_filename

    def get_seg_tarfile(self):
        """Gets tar file that was used during template creation"""
        return self._seg_tarfile

    def get_number_new_segments(self):
        """ Gets the number of new segments added """
        return self._number_new_segments

    def get_original_primary_count(self):
        """Returns the original number of primary segments"""
        return self._original_primary_count

    def get_gp_segment_configuration_backup(self):
        """Gets the filename of the gp_segment_configuration backup file
        created during expansion setup"""
        return self._gp_segment_configuration_backup

    def set_gp_segment_configuration_backup(self, filename):
        """Sets the filename of the gp_segment_configuration backup file"""
        self._gp_segment_configuration_backup = filename

    def is_standby(self):
        """Returns True if running on standby"""
        return os.path.exists(self._master_data_directory + self._status_standby_filename)


# -------------------------------------------------------------------------

class ExpansionError(Exception): pass


class SegmentTemplateError(Exception): pass


# -------------------------------------------------------------------------
class SegmentTemplate:
    """Class for creating, distributing and deploying new segments to an
    existing GPDB array"""

    def __init__(self, logger, statusLogger, pool,
                 gparray, masterDataDirectory,
                 dburl, conn, noVacuumCatalog, tempDir, batch_size,
                 segTarDir='.', schemaTarFile='gpexpand_schema.tar'):
        self.logger = logger
        self.statusLogger = statusLogger
        self.pool = pool
        self.gparray = gparray
        self.noVacuumCatalog = noVacuumCatalog
        self.tempDir = tempDir
        self.batch_size = batch_size
        self.dburl = dburl
        self.conn = conn
        self.masterDataDirectory = masterDataDirectory
        self.schema_tar_file = schemaTarFile
        self.maxDbId = self.gparray.get_max_dbid()
        self.segTarDir = segTarDir
        self.segTarFile = os.path.join(segTarDir, self.schema_tar_file)

        hosts = []
        for seg in self.gparray.getExpansionSegDbList():
            hosts.append(seg.getSegmentHostName())
        self.hosts = SegmentTemplate.consolidate_hosts(pool, hosts)
        logger.debug('Hosts: %s' % self.hosts)

    @staticmethod
    def consolidate_hosts(pool, hosts):
        tmpHosts = {}
        consolidatedHosts = []

        for host in hosts:
            tmpHosts[host] = 0

        for host in tmpHosts.keys():
            hostnameCmd = Hostname('gpexpand associating hostnames with segments', ctxt=REMOTE, remoteHost=host)
            pool.addCommand(hostnameCmd)

        pool.join()

        finished_cmds = pool.getCompletedItems()

        for cmd in finished_cmds:
            if not cmd.was_successful():
                raise SegmentTemplateError(cmd.get_results())
            if cmd.get_hostname() not in consolidatedHosts:
                logger.debug('Adding %s to host list' % cmd.get_hostname())
                consolidatedHosts.append(cmd.get_hostname())

        return consolidatedHosts

    def build_segment_template(self):
        """Builds segment template tar file"""
        self.statusLogger.set_status('BUILD_SEGMENT_TEMPLATE_STARTED', self.tempDir)
        self._create_template()
        self._fixup_template()
        self._tar_template()
        self.statusLogger.set_status('BUILD_SEGMENT_TEMPLATE_DONE')

    def build_new_segments(self):
        """Deploys the template tar file and configures the new segments"""
        self.statusLogger.set_status('BUILD_SEGMENTS_STARTED', self.segTarFile)
        self._distribute_template()
        self._configure_new_segments()
        numNewSegments = len(self.gparray.getExpansionSegDbList())
        self.statusLogger.set_status('BUILD_SEGMENTS_DONE', numNewSegments)

    def _create_template(self):
        """Creates the schema template that is used by new segments"""
        self.logger.info('Creating segment template')

        if not self.noVacuumCatalog:
            self.logger.info('VACUUM FULL on the catalog tables')
            catalog.vacuum_catalog(self.dburl, self.conn, full=True, utility=True)

        MakeDirectory.local('gpexpand create temp dir', self.tempDir)

        self._select_src_segment()

        self.oldSegCount = self.gparray.get_segment_count()

        self.conn.close()

        GpStop.local('gpexpand _create_template stop gpdb', masterOnly=True, fast=True)

        # Verify that we actually stopped
        self.logger.debug('Validating array state')
        pgControlDataCmd = PgControlData('Validate stopped', self.masterDataDirectory)
        state = None
        try:
            pgControlDataCmd.run(validateAfter=True)
        except Exception, e:
            raise SegmentTemplateError(e)
        state = pgControlDataCmd.get_value('Database cluster state')
        if state != 'shut down':
            raise SegmentTemplateError('Failed to stop the array.  pg_controldata return state of %s' % state)

        try:
            masterSeg = self.gparray.master
            masterSeg.createTemplate(dstDir=self.tempDir)
        except Exception, msg:
            raise SegmentTemplateError(msg)

    def _select_src_segment(self):
        """Gets a segment to use as a source for pg_hba.conf
        and postgresql.conf files"""
        seg = self.gparray.segments[0]
        if seg.primaryDB.valid:
            self.srcSegHostname = seg.primaryDB.getSegmentHostName()
            self.srcSegDataDir = seg.primaryDB.getSegmentDataDirectory()
        elif seg.mirrorDBs[0] is not None and seg.mirrorDBs[0].valid:
            self.srcSegHostname = seg.mirrorDBs[0].getSegmentHostName()
            self.srcSegDataDir = seg.mirrorDBs[0].getSegmentDataDirectory()
        else:
            raise SegmentTemplateError("no valid segdb for content=0 to use as a template")

    def _distribute_template(self):
        """Distributes the template tar file to the new segments and expands it"""
        self.logger.info('Distributing template tar file to new hosts')

        self._distribute_tarfile()

    def _distribute_tarfile(self):
        """Distributes template tar file to hosts"""
        for host in self.hosts:
            logger.debug('Copying tar file to %s' % host)
            cpCmd = RemoteCopy('gpexpand distribute tar file to new hosts', self.schema_tar_file, host, self.segTarDir)
            self.pool.addCommand(cpCmd)

        self.pool.join()
        self.pool.check_results()

    def _configure_new_segments(self):
        """Configures new segments.  This includes modifying the postgresql.conf file
        and setting up the gp_id table"""

        self.logger.info('Configuring new segments (primary)')
        new_segment_info = ConfigureNewSegment.buildSegmentInfoForNewSegment(self.gparray.getExpansionSegDbList(),
                                                                             primaryMirror='primary')
        for host in iter(new_segment_info):
            segCfgCmd = ConfigureNewSegment(name='gpexpand configure new segments', confinfo=new_segment_info[host],
                                            tarFile=self.segTarFile, newSegments=True,
                                            verbose=gplog.logging_is_verbose(), batchSize=self.batch_size,
                                            ctxt=REMOTE, remoteHost=host)
            self.pool.addCommand(segCfgCmd)

        self.pool.join()
        self.pool.check_results()

        self.logger.info('Configuring new segments (mirror)')
        new_segment_info = ConfigureNewSegment.buildSegmentInfoForNewSegment(self.gparray.getExpansionSegDbList(),
                                                                             primaryMirror='mirror')
        for host in iter(new_segment_info):
            segCfgCmd = ConfigureNewSegment(name='gpexpand configure new segments', confinfo=new_segment_info[host],
                                            tarFile=self.schema_tar_file, newSegments=True,
                                            verbose=gplog.logging_is_verbose(), batchSize=self.batch_size,
                                            ctxt=REMOTE, remoteHost=host, validationOnly=True)
            self.pool.addCommand(segCfgCmd)

        self.pool.join()
        self.pool.check_results()

    def _get_transaction_filespace_dir(self, transaction_flat_file):
        filespace_dir = None

        with open(transaction_flat_file) as tfile:
            for line in tfile:
                fs_info = line.split()
                if len(fs_info) != 2:
                    continue
                filespace_dir = fs_info[1]

        return filespace_dir

    def _fixup_template(self):
        """Copies postgresql.conf and pg_hba.conf files from a valid segment on the system.
        Then modifies the template copy of pg_hba.conf"""

        self.logger.info('Copying postgresql.conf from existing segment into template')

        localHostname = self.gparray.master.getSegmentHostName()
        cpCmd = RemoteCopy(
            'gpexpand copying postgresql.conf to %s:%s/postgresql.conf' % (self.srcSegHostname, self.srcSegDataDir),
            self.srcSegDataDir + '/postgresql.conf', localHostname,
            self.tempDir, ctxt=REMOTE, remoteHost=self.srcSegHostname)
        cpCmd.run(validateAfter=True)

        self.logger.info('Copying pg_hba.conf from existing segment into template')
        cpCmd = RemoteCopy('gpexpand copy pg_hba.conf to %s:%s/pg_hba.conf' % (self.srcSegHostname, self.srcSegDataDir),
                           self.srcSegDataDir + '/pg_hba.conf', localHostname,
                           self.tempDir, ctxt=REMOTE, remoteHost=self.srcSegHostname)
        cpCmd.run(validateAfter=True)

        # Copy the transaction directories into template
        pg_system_filespace_entries = GetFilespaceEntriesDict(GetFilespaceEntries(self.gparray,
                                                                                  PG_SYSTEM_FILESPACE).run()).run()
        transaction_flat_file = os.path.join(pg_system_filespace_entries[1][2], GP_TRANSACTION_FILES_FILESPACE)
        filespace_dir = None
        if os.path.exists(transaction_flat_file):
            filespace_dir = self._get_transaction_filespace_dir(transaction_flat_file)
            logger.debug('Filespace location = %s' % filespace_dir)

            if filespace_dir:
                transaction_files_dir = ['pg_xlog', 'pg_multixact', 'pg_subtrans', 'pg_clog',
                                         'pg_distributedlog', 'pg_distributedxidmap']
                for directory in transaction_files_dir:
                    dst_dir = os.path.join(self.tempDir, directory)
                    src_dir = os.path.join(filespace_dir, directory)

                    mkCmd = MakeDirectory('gpexpand creating transaction directories in template', dst_dir)
                    mkCmd.run(validateAfter=True)
                    cpCmd = LocalDirCopy('gpexpand copying dir %s' % src_dir, src_dir, dst_dir)
                    cpCmd.run(validateAfter=True)

        # Don't need log files and gpperfmon files in template.
        rmCmd = RemoveDirectory('gpexpand remove gppermfon data from template',
                                self.tempDir + '/gpperfmon/data')
        rmCmd.run(validateAfter=True)
        rmCmd = RemoveDirectoryContents('gpexpand remove logs from template',
                                        self.tempDir + '/pg_log')
        rmCmd.run(validateAfter=True)

        # other files not needed
        rmCmd = RemoveFile('gpexpand remove postmaster.opt from template',
                            self.tempDir + '/postmaster.opts')
        rmCmd.run(validateAfter=True)
        rmCmd = RemoveFile('gpexpand remove postmaster.pid from template',
                            self.tempDir + '/postmaster.pid')
        rmCmd.run(validateAfter=True)
        rmCmd = RemoveGlob('gpexpand remove gpexpand files from template',
                            self.tempDir + '/gpexpand.*')
        rmCmd.run(validateAfter=True)

        # We dont need the flat files
        rmCmd = RemoveFile('gpexpand remove transaction flat file from template',
                            self.tempDir + '/' + GP_TRANSACTION_FILES_FILESPACE)
        rmCmd.run(validateAfter=True)
        rmCmd = RemoveFile('gpexpand remove temporary flat file from template',
                            self.tempDir + '/' + GP_TEMPORARY_FILES_FILESPACE)
        rmCmd.run(validateAfter=True)

        self.logger.info('Adding new segments into template pg_hba.conf')
        try:
            fp = open(self.tempDir + '/pg_hba.conf', 'a')
            try:
                new_host_set = set()
                for newSeg in self.gparray.getExpansionSegDbList() + self.gparray.getDbList():
                    host = newSeg.getSegmentHostName()
                    new_host_set.add(host)

                for new_host in new_host_set:
                    addrinfo = socket.getaddrinfo(new_host, None)
                    ipaddrlist = list(set([(ai[0], ai[4][0]) for ai in addrinfo]))
                    fp.write('# %s\n' % new_host)
                    for addr in ipaddrlist:
                        fp.write(
                            'host\tall\tall\t%s/%s\ttrust\n' % (addr[1], '32' if addr[0] == socket.AF_INET else '128'))

            finally:
                fp.close()
        except IOError:
            raise SegmentTemplateError('Failed to open %s/pg_hba.conf' % self.tempDir)
        except Exception:
            raise SegmentTemplateError('Failed to add new segments to template pg_hba.conf')

    def _tar_template(self):
        """Tars up the template files"""
        self.logger.info('Creating schema tar file')
        tarCmd = CreateTar('gpexpand tar segment template', self.tempDir, self.schema_tar_file)
        tarCmd.run(validateAfter=True)

    @staticmethod
    def cleanup_build_segment_template(tarFile, tempDir):
        """Reverts the work done by build_segment_template.  Deletes the temp
        directory and local tar file"""
        rmCmd = RemoveDirectory('gpexpand remove temp dir: %s' % tempDir, tempDir)
        rmCmd.run(validateAfter=True)
        rmCmd = RemoveFile('gpexpand remove segment template file', tarFile)
        rmCmd.run(validateAfter=True)

    @staticmethod
    def cleanup_build_new_segments(pool, tarFile, gparray, hosts=None, removeDataDirs=False):
        """Cleans up the work done by build_new_segments.  Deletes remote tar files and
        and removes remote data directories"""

        if not hosts:
            hosts = []
            for seg in gparray.getExpansionSegDbList():
                hosts.append(seg.getSegmentHostName())

        # Remove template tar file
        for host in hosts:
            rmCmd = RemoveFile('gpexpand remove segment template file on host: %s' % host,
                               tarFile, ctxt=REMOTE, remoteHost=host)
            pool.addCommand(rmCmd)

        if removeDataDirs:
            for seg in gparray.getExpansionSegDbList():
                hostname = seg.getSegmentHostName()
                filespaces = seg.getSegmentFilespaces()
                for oid in filespaces:
                    datadir = filespaces[oid]
                    rmCmd = RemoveDirectory('gpexpand remove new segment data directory: %s:%s' % (hostname, datadir),
                                            datadir, ctxt=REMOTE, remoteHost=hostname)
                    pool.addCommand(rmCmd)
        pool.join()
        pool.check_results()

    def cleanup(self):
        """Cleans up temporary files from the local system and new segment hosts"""

        self.logger.info('Cleaning up temporary template files')
        SegmentTemplate.cleanup_build_segment_template(self.schema_tar_file, self.tempDir)
        SegmentTemplate.cleanup_build_new_segments(self.pool, self.segTarFile, self.gparray, self.hosts)


# ------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------
class NewSegmentInput:
    def __init__(self, hostname, address, port, datadir, dbid, contentId, role, replicationPort=None, fileSpaces=None):
        self.hostname = hostname
        self.address = address
        self.port = port
        self.datadir = datadir
        self.dbid = dbid
        self.contentId = contentId
        self.role = role
        self.replicationPort = replicationPort
        self.fileSpaces = fileSpaces


# ------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------
class gpexpand:
    def __init__(self, logger, gparray, dburl, options, parallel=1):
        self.pastThePointOfNoReturn = False
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.numworkers = parallel
        self.gparray = gparray
        self.unique_index_tables = {}
        self.conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8', allowSystemTableMods='dml')
        self.old_segments = self.gparray.getSegDbList()
        if dburl.pgdb == 'template0' or dburl.pgdb == 'template1' or dburl.pgdb == 'postgres':
            raise ExpansionError("Invalid database '%s' specified.  Cannot use a template database.\n"
                                 "Please set the environment variable PGDATABASE to a different "
                                 "database or use the -D option to specify a database and re-run" % dburl.pgdb)

        datadir = self.gparray.master.getSegmentDataDirectory()
        self.statusLogger = GpExpandStatus(logger=logger,
                                           master_data_directory=datadir,
                                           master_mirror=self.gparray.standbyMaster)

        # Adjust batch size if it's too high given the number of segments
        seg_count = len(self.old_segments)
        if self.options.batch_size > seg_count:
            self.options.batch_size = seg_count
        self.pool = WorkerPool(numWorkers=self.options.batch_size)

        self.tempDir = self.statusLogger.get_temp_dir()
        if not self.tempDir:
            self.tempDir = createTempDirectoryName(self.options.master_data_directory, "gpexpand")
        self.queue = None
        self.segTemplate = None
        pass

    @staticmethod
    def prepare_gpdb_state(logger, dburl, options):
        """ Gets GPDB in the appropriate state for an expansion.
        This state will depend on if this is a new expansion setup,
        a continuation of a previous expansion or a rollback """
        # Get the database in the expected state for the expansion/rollback
        status_file_exists = os.path.exists(options.master_data_directory + '/gpexpand.status')
        gpexpand_db_status = None

        if status_file_exists:
            # gpexpand status file exists so the last run of gpexpand didn't finish properly
            gpexpand.get_gpdb_in_state(GPDB_UTILITY, options)
        else:
            gpexpand.get_gpdb_in_state(GPDB_STARTED, options)

            logger.info('Querying gpexpand schema for current expansion state')
            try:
                gpexpand_db_status = gpexpand.get_status_from_db(dburl, options)
            except Exception, e:
                raise Exception('Error while trying to query the gpexpand schema: %s' % e)
            logger.debug('Expansion status returned is %s' % gpexpand_db_status)

            if (not gpexpand_db_status and options.filename) and not options.clean:
                # New expansion, need to be in master only
                logger.info('Readying Greenplum Database for a new expansion')
                gpexpand.get_gpdb_in_state(GPDB_UTILITY, options)

        return gpexpand_db_status

    @staticmethod
    def get_gpdb_in_state(state, options):
        runningStatus = chk_local_db_running(options.master_data_directory, options.pgport)
        gpdb_running = runningStatus[0] and runningStatus[1] and runningStatus[2] and runningStatus[3]
        if gpdb_running:
            gpdb_mode = get_local_db_mode(options.master_data_directory)

        if state == GPDB_STARTED:
            if gpdb_running:
                if gpdb_mode != 'UTILITY':
                    return
                else:
                    GpStop.local('Stop GPDB', masterOnly=True, fast=True)
            GpStart.local('Start GPDB')
        elif state == GPDB_STOPPED:
            if gpdb_running:
                if gpdb_mode != 'UTILITY':
                    GpStop.local('Stop GPDB', fast=True)
                else:
                    GpStop.local('Stop GPDB', masterOnly=True, fast=True)
        elif state == GPDB_UTILITY:
            if gpdb_running:
                if gpdb_mode == 'UTILITY':
                    return
                GpStop.local('Stop GPDB', fast=True)
            GpStart.local('Start GPDB in master only mode', masterOnly=True)
        else:
            raise Exception('Unkown gpdb state')

    @staticmethod
    def get_status_from_db(dburl, options):
        """Gets gpexpand status from the gpexpand schema"""
        status_conn = None
        gpexpand_db_status = None
        if get_local_db_mode(options.master_data_directory) == 'NORMAL':
            try:
                status_conn = dbconn.connect(dburl, encoding='UTF8')
                # Get the last status entry
                cursor = dbconn.execSQL(status_conn, 'SELECT status FROM gpexpand.status ORDER BY updated DESC LIMIT 1')
                if cursor.rowcount == 1:
                    gpexpand_db_status = cursor.fetchone()[0]

            except Exception:
                # expansion schema doesn't exists or there was a connection failure.
                pass
            finally:
                if status_conn: status_conn.close()

        # make sure gpexpand schema doesn't exist since it wasn't in DB provided
        if not gpexpand_db_status:
            """
            MPP-14145 - If there's no discernable status, the schema must not exist.

            The checks in get_status_from_db claim to look for existence of the 'gpexpand' schema, but more accurately they're
            checking for non-emptiness of the gpexpand.status table. If the table were empty, but the schema did exist, gpexpand would presume
            a new expansion was taking place and it would try to CREATE SCHEMA later, which would fail. So, here, if this is the case, we error out.

            Note: -c/--clean will not necessarily work either, as it too has assumptions about the non-emptiness of the gpexpand schema.
            """
            with dbconn.connect(dburl, encoding='UTF8', utility=True) as conn:
                count = dbconn.execSQLForSingleton(conn,
                                                   "SELECT count(n.nspname) FROM pg_catalog.pg_namespace n WHERE n.nspname = 'gpexpand'")
                if count > 0:
                    raise ExpansionError(
                        "Existing expansion state could not be determined, but a gpexpand schema already exists. Cannot proceed.")

            # now determine whether gpexpand schema merely resides in another DB
            status_conn = dbconn.connect(dburl, encoding='UTF8')
            db_list = catalog.getDatabaseList(status_conn)
            status_conn.close()

            for db in db_list:
                dbname = db[0]
                if dbname in ['template0', 'template1', 'postgres', dburl.pgdb]:
                    continue
                logger.debug('Looking for gpexpand schema in %s' % dbname.decode('utf-8'))
                test_url = copy.deepcopy(dburl)
                test_url.pgdb = dbname
                c = dbconn.connect(test_url, encoding='UTF8')
                try:
                    cursor = dbconn.execSQL(c, 'SELECT status FROM gpexpand.status ORDER BY updated DESC LIMIT 1')
                except:
                    # Not in here
                    pass
                else:
                    raise ExpansionError("""gpexpand schema exists in database %s, not in %s.
Set PGDATABASE or use the -D option to specify the correct database to use.""" % (
                        dbname.decode('utf-8'), options.database))
                finally:
                    if c:
                        c.close()

        return gpexpand_db_status

    def validate_max_connections(self):
        try:
            conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8')
            max_connections = int(catalog.getSessionGUC(conn, 'max_connections'))
        except DatabaseError, ex:
            if self.options.verbose:
                logger.exception(ex)
            logger.error('Failed to check max_connections GUC')
            if conn: conn.close()
            raise ex

        if max_connections < self.options.parallel * 2 + 1:
            self.logger.error('max_connections is too small to expand %d tables at' % self.options.parallel)
            self.logger.error('a time.  This will lead to connection errors.  Either')
            self.logger.error('reduce the value for -n passed to gpexpand or raise')
            self.logger.error('max_connections in postgresql.conf')
            return False

        return True

    def validate_unalterable_tables(self):
        conn = None
        unalterable_tables = []

        try:
            conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8')
            databases = catalog.getDatabaseList(conn)
            conn.close()

            tempurl = copy.deepcopy(self.dburl)
            for db in databases:
                if db[0] == 'template0':
                    continue
                self.logger.info('Checking database %s for unalterable tables...' % db[0].decode('utf-8'))
                tempurl.pgdb = db[0]
                conn = dbconn.connect(tempurl, utility=True, encoding='UTF8')
                cursor = dbconn.execSQL(conn, unalterable_table_sql)
                for row in cursor:
                    unalterable_tables.append(row)
                cursor.close()
                conn.close()

        except DatabaseError, ex:
            if self.options.verbose:
                logger.exception(ex)
            logger.error('Failed to check for unalterable tables.')
            if conn: conn.close()
            raise ex

        if len(unalterable_tables) > 0:
            self.logger.error('The following tables cannot be altered because they contain')
            self.logger.error('dropped columns of user defined types:')
            for t in unalterable_tables:
                self.logger.error('\t%s.%s' % (t[0].decode('utf-8'), t[1].decode('utf-8')))
            self.logger.error('Please consult the documentation for instructions on how to')
            self.logger.error('correct this issue, then run gpexpand again')
            return False

        return True

    def check_unique_indexes(self):
        """ Checks if there are tables with unique indexes.
        Returns true if unique indexes exist"""

        conn = None
        has_unique_indexes = False

        try:
            conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8')
            databases = catalog.getDatabaseList(conn)
            conn.close()

            tempurl = copy.deepcopy(self.dburl)
            for db in databases:
                if db[0] == 'template0':
                    continue
                self.logger.info('Checking database %s for tables with unique indexes...' % db[0].decode('utf-8'))
                tempurl.pgdb = db[0]
                conn = dbconn.connect(tempurl, utility=True, encoding='UTF8')
                cursor = dbconn.execSQL(conn, has_unique_index_sql)
                for row in cursor:
                    has_unique_indexes = True
                    self.unique_index_tables[row[0]] = True
                cursor.close()
                conn.close()

        except DatabaseError, ex:
            if self.options.verbose:
                logger.exception(ex)
            logger.error('Failed to check for unique indexes.')
            if conn: conn.close()
            raise ex

        return has_unique_indexes

    def rollback(self, dburl):
        """Rolls back and expansion setup that didn't successfully complete"""
        cleanSchema = False
        status_history = self.statusLogger.get_status_history()
        if not status_history:
            raise ExpansionError('No status history to rollback.')

        if (status_history[-1])[0] == 'EXPANSION_PREPARE_DONE':
            raise ExpansionError('Expansion preparation complete.  Nothing to rollback')

        for status in reversed(status_history):
            if status[0] == 'BUILD_SEGMENT_TEMPLATE_STARTED':
                if self.statusLogger.is_standby():
                    self.logger.info('Running on standby master, skipping segment template rollback')
                    continue
                self.logger.info('Rolling back segment template build')
                SegmentTemplate.cleanup_build_segment_template('gpexpand_schema.tar', status[1])

            elif status[0] == 'BUILD_SEGMENTS_STARTED':
                self.logger.info('Rolling back building of new segments')
                newSegList = self.read_input_files(self.statusLogger.get_input_filename())
                self.addNewSegments(newSegList)
                SegmentTemplate.cleanup_build_new_segments(self.pool,
                                                           self.statusLogger.get_seg_tarfile(),
                                                           self.gparray, removeDataDirs=True)

            elif status[0] == 'UPDATE_OLD_SEGMENTS_STARTED':
                self.logger.info('Rolling back update of original segments')
                self.restore_original_segments()

            elif status[0] == 'UPDATE_CATALOG_STARTED':
                self.logger.info('Rolling back master update')
                self.restore_master()
                self.gparray = GpArray.initFromCatalog(dburl, utility=True)

            elif status[0] == 'SETUP_EXPANSION_SCHEMA_STARTED':
                cleanSchema = True
            else:
                self.logger.debug('Skipping %s' % status[0])

        self.conn.close()

        GpStop.local('gpexpand rollback', masterOnly=True, fast=True)

        if cleanSchema:
            GpStart.local('gpexpand rollback start database restricted', restricted=True)
            self.logger.info('Dropping expansion expansion schema')
            schema_conn = dbconn.connect(self.dburl, encoding='UTF8', allowSystemTableMods='dml')
            try:
                dbconn.execSQL(schema_conn, drop_schema_sql)
                schema_conn.commit()
                schema_conn.close()
            except:
                pass  # schema wasn't created yet.
            GpStop('gpexpand rollback stop database', fast=True)

        self.statusLogger.remove_status_file()
        self.statusLogger.remove_segment_configuration_backup_file()

    def get_state(self):
        """Returns expansion state from status logger"""
        return self.statusLogger.get_current_status()[0]

    def generate_inputfile(self):
        """Writes a gpexpand input file based on expansion segments
        added to gparray by the gpexpand interview"""
        outputfile = 'gpexpand_inputfile_' + strftime("%Y%m%d_%H%M%S")
        outfile = open(outputfile, 'w')

        logger.info("Generating input file...")

        for db in self.gparray.getExpansionSegDbList():
            tempStr = "%s:%s:%d:%s:%d:%d:%s" % (canonicalize_address(db.getSegmentHostName())
                                                , canonicalize_address(db.getSegmentAddress())
                                                , db.getSegmentPort()
                                                , db.getSegmentDataDirectory()
                                                , db.getSegmentDbId()
                                                , db.getSegmentContentId()
                                                , db.getSegmentPreferredRole()
                                                )
            if db.getSegmentReplicationPort() != None:
                tempStr = tempStr + ':' + str(db.getSegmentReplicationPort())
            outfile.write(tempStr + "\n")

        outfile.close()

        return outputfile

    # ------------------------------------------------------------------------
    def generate_filespaces_inputfile(self, outFileNamePrefix):
        """
        Writes a gpexpand filespace input file based on expansion segments
        added to gparray by the gpexpand interview. If the new segments
        contain filespaces, then return the name of the file, else return
        None.
        """
        filespaces = self.gparray.getFilespaces(includeSystemFilespace=False)
        if filespaces != None and len(filespaces) > 0:
            outputfile = outFileNamePrefix + FILE_SPACES_INPUT_FILENAME_SUFFIX
        else:
            outputfile = None

        if outputfile != None:
            outfileFD = open(outputfile, 'w')

            logger.info("Generating filespaces input file...")

            firstLine = FILE_SPACES_INPUT_FILE_LINE_1_PREFIX + "="
            firstFs = True
            for fs in filespaces:
                if firstFs == True:
                    firstLine = firstLine + fs.getName()
                    firstFs = False
                else:
                    firstLine = firstLine + ":" + fs.getName()
            outfileFD.write(firstLine + '\n')

            for db in self.gparray.getExpansionSegDbList():
                dbid = db.getSegmentDbId()
                outLine = str(dbid)
                segmentFilespaces = db.getSegmentFilespaces()
                for fs in filespaces:
                    oid = fs.getOid()
                    path = segmentFilespaces[oid]
                    outLine = outLine + ":" + path
                outfileFD.write(outLine + '\n')

            outfileFD.close()

        return outputfile

    def addNewSegments(self, inputFileEntryList):
        for seg in inputFileEntryList:
            self.gparray.addExpansionSeg(content=int(seg.contentId)
                                         , preferred_role=seg.role
                                         , dbid=int(seg.dbid)
                                         , role=seg.role
                                         , hostname=seg.hostname.strip()
                                         , address=seg.address.strip()
                                         , port=int(seg.port)
                                         , datadir=os.path.abspath(seg.datadir.strip())
                                         , replication_port=seg.replicationPort
                                         , fileSpaces=seg.fileSpaces
                                         )
        try:
            self.gparray.validateExpansionSegs()
        except Exception, e:
            raise ExpansionError('Invalid input file: %s' % e)

    def read_input_files(self, inputFilename=None):
        """Reads and validates line format of the input file passed
        in on the command line via the -i arg"""

        retValue = []

        if not self.options.filename and not inputFilename:
            raise ExpansionError('Missing input file')

        if self.options.filename:
            inputFilename = self.options.filename
        fsInputFilename = inputFilename + FILE_SPACES_INPUT_FILENAME_SUFFIX
        fsOidList = []
        fsDictionary = {}
        f = None
        try:
            existsCmd = FileDirExists(name="gpexpand see if .fs file exists", directory=fsInputFilename)
            existsCmd.run(validateAfter=True)
            exists = existsCmd.filedir_exists()
            if exists == False and len(self.gparray.getFilespaces(includeSystemFilespace=False)) != 0:
                raise ExpansionError("Expecting filespaces input file: " + fsInputFilename)
            if exists == True:
                f = open(fsInputFilename, 'r')
                for lineNumber, l in line_reader(f):
                    if lineNumber == 1:
                        fsNameString = l.strip().split("=")
                        fsNameList = fsNameString[1].strip().split(":")
                        for name in fsNameList:
                            oid = self.gparray.getFileSpaceOid(name)
                            if oid == None:
                                raise ExpansionError("Unknown filespace name: " + str(name))
                            fsOidList.append(oid)
                        # Make sure all the filespace names are specified.
                        if len(fsNameList) != len(self.gparray.getFilespaces(includeSystemFilespace=False)):
                            missingFsNames = []
                            filespaces = self.gparray.getFilespaces()
                            for fs in filespaces:
                                if fs.getName() not in fsNameList:
                                    missingFsNames.append(fs.getName())
                            raise ExpansionError("Missing filespaces: " + str(missingFsNames))

                    else:
                        fsLine = l.strip().split(":")
                        try:
                            fsDictionary[fsLine[0]] = fsLine[1:]
                        except Exception, e:
                            raise ExpansionError("Problem with inputfile %s, line number %s, exceptin %s." % \
                                                 (fsInputFilename, str(lineNumber), str(e)))

        except IOError, ioe:
            raise ExpansionError('Problem with filespace input file: %s. Exception: %s' % (fsInputFilename, str(ioe)))
        finally:
            if f != None:
                f.close()

        try:
            f = open(inputFilename, 'r')
            try:
                for line, l in line_reader(f):

                    hostname, address, port, datadir, dbid, contentId, role, replicationPort \
                        = parse_gpexpand_segment_line(inputFilename, line, l)

                    filespaces = {}
                    if len(fsDictionary) > 0:
                        fileSpacesPathList = fsDictionary[dbid]
                    else:
                        fileSpacesPathList = []
                    index = 0
                    for oid in fsOidList:
                        filespaces[oid] = fileSpacesPathList[index]
                        index = index + 1

                    # Check that input values look reasonable.
                    if hostname == None or len(hostname) == 0:
                        raise ExpansionError("Invalid host name on line " + str(line))
                    if address == None or len(address) == 0:
                        raise ExpansionError("Invaid address on line " + str(line))
                    if port == None or str(port).isdigit() == False or int(port) < 0:
                        raise ExpansionError("Invalid port number on line " + str(line))
                    if datadir == None or len(datadir) == 0:
                        raise ExpansionError("Invalid data directory on line " + str(line))
                    if dbid == None or str(dbid).isdigit() == False or int(dbid) < 0:
                        raise ExpansionError("Invalid dbid on line " + str(line))
                    if contentId == None or str(contentId).isdigit() == False or int(contentId) < 0:
                        raise ExpansionError("Invalid contentId on line " + str(line))
                    if role == None or len(role) > 1 or (role != 'p' and role != 'm'):
                        raise ExpansionError("Invalid role on line " + str(line))
                    if replicationPort != None and int(replicationPort) < 0:
                        raise ExpansionError("Invalid replicationPort on line " + str(line))

                    retValue.append(NewSegmentInput(hostname=hostname
                                                    , port=port
                                                    , address=address
                                                    , datadir=datadir
                                                    , dbid=dbid
                                                    , contentId=contentId
                                                    , role=role
                                                    , replicationPort=replicationPort
                                                    , fileSpaces=filespaces
                                                    ))
            except ValueError:
                raise ExpansionError('Missing or invalid value on line %d.' % line)
            except Exception, e:
                raise ExpansionError('Invalid input file on line %d: %s' % (line, str(e)))
            finally:
                f.close()
            return retValue
        except IOError:
            raise ExpansionError('Input file %s not found' % self.options.filename)

    def add_segments(self):
        """Starts the process of adding the new segments to the array"""
        self.segTemplate = SegmentTemplate(logger=self.logger,
                                           statusLogger=self.statusLogger,
                                           pool=self.pool,
                                           gparray=self.gparray,
                                           masterDataDirectory=self.options.master_data_directory,
                                           dburl=self.dburl,
                                           conn=self.conn,
                                           noVacuumCatalog=self.options.novacuum,
                                           tempDir=self.tempDir,
                                           segTarDir=self.options.tardir,
                                           batch_size=self.options.batch_size)
        try:
            self.segTemplate.build_segment_template()
            self.segTemplate.build_new_segments()
        except SegmentTemplateError, msg:
            raise ExpansionError(msg)

    def update_original_segments(self):
        """Updates the pg_hba.conf file and updates the gp_id catalog table
        of existing hosts"""
        self.statusLogger.set_status('UPDATE_OLD_SEGMENTS_STARTED', self.gparray.get_primary_count())

        self.logger.info('Backing up pg_hba.conf file on original segments')

        # backup pg_hba.conf file on original segments
        for seg in self.old_segments:
            if seg.isSegmentQD() or seg.getSegmentStatus() != 'u':
                continue

            hostname = seg.getSegmentHostName()
            datadir = seg.getSegmentDataDirectory()

            srcFile = datadir + '/pg_hba.conf'
            dstFile = datadir + '/pg_hba.gpexpand.bak'
            cpCmd = RemoteCopy('gpexpand back up pg_hba.conf file on original segments',
                               srcFile, hostname, dstFile, ctxt=REMOTE, remoteHost=hostname)

            self.pool.addCommand(cpCmd)

        self.pool.join()

        try:
            self.pool.check_results()
        except ExecutionError, msg:
            raise ExpansionError('Failed to configure original segments: %s' % msg)

        # Copy the new pg_hba.conf file to original segments
        self.logger.info('Copying new pg_hba.conf file to original segments')
        for seg in self.old_segments:
            if seg.isSegmentQD() or seg.getSegmentStatus() != 'u':
                continue

            hostname = seg.getSegmentHostName()
            datadir = seg.getSegmentDataDirectory()

            cpCmd = RemoteCopy('gpexpand copy new pg_hba.conf file to original segments',
                               self.tempDir + '/pg_hba.conf', hostname, datadir)

            self.pool.addCommand(cpCmd)

        self.pool.join()

        try:
            self.pool.check_results()
        except ExecutionError, msg:
            raise ExpansionError('Failed to configure original segments: %s' % msg)

        # Update the gp_id of original segments
        self.newPrimaryCount = 0;
        for seg in self.gparray.getExpansionSegDbList():
            if seg.isSegmentPrimary(False):
                self.newPrimaryCount += 1

        self.newPrimaryCount += self.gparray.get_primary_count()

        self.logger.info('Configuring original segments')

        if self.segTemplate:
            self.segTemplate.cleanup()

        self.statusLogger.set_status('UPDATE_OLD_SEGMENTS_DONE')

    def restore_original_segments(self):
        """ Restores the original segments back to their state prior the expansion
        setup.  This is only possible if the expansion setup has not completed
        successfully."""
        self.logger.info('Restoring original segments')
        gp_segment_configuration_backup_file = self.statusLogger.get_gp_segment_configuration_backup();
        if gp_segment_configuration_backup_file:
            originalArray = GpArray.initFromFile(self.statusLogger.get_gp_segment_configuration_backup())
        else:
            originalArray = self.gparray

        # Restore pg_hba.conf file from backup
        self.logger.info('Restoring pg_hba.conf file on original segments')
        for seg in originalArray.getSegDbList():
            datadir = seg.getSegmentDataDirectory()
            hostname = seg.getSegmentHostName()

            srcFile = datadir + '/pg_hba.gpexpand.bak'
            dstFile = datadir + '/pg_hba.conf'
            cpCmd = RemoteCopy('gpexpand restore of pg_hba.conf file on original segments',
                               srcFile, hostname, dstFile, ctxt=REMOTE,
                               remoteHost=hostname)

            self.pool.addCommand(cpCmd)

        self.pool.join()

        try:
            self.pool.check_results()
        except:
            # Setup didn't get this far so no backup to restore.
            self.pool.empty_completed_items()

        # note: this code may not be needed -- it will NOT change gp_id
        #       However, the call to gpconfigurenewsegment may still be doing some needed work (stopping the segment)
        #       which could be unnecessary or could be moved here)
        self.logger.info('Restoring original segments catalog tables')
        orig_segment_info = ConfigureNewSegment.buildSegmentInfoForNewSegment(originalArray.getSegDbList())
        for host in iter(orig_segment_info):
            segCfgCmd = ConfigureNewSegment(name='gpexpand configure new segments', confinfo=orig_segment_info[host],
                                            verbose=gplog.logging_is_verbose(), batchSize=self.options.batch_size,
                                            ctxt=REMOTE, remoteHost=host)
            self.pool.addCommand(segCfgCmd)

        self.pool.join()

        try:
            self.pool.check_results()
        except ExecutionError:
            raise ExpansionError('Failed to restore original segments')

    def _construct_filespace_parameter(self, seg, gpFSobjList):
        """ return a string containing a filespace parameter appropriate for use in sql functions. """
        filespaces = []
        segFilespaces = seg.getSegmentFilespaces()
        filespaceNames = []
        filespaceLocations = []
        for entry in gpFSobjList:
            name = entry.getName()
            oid = entry.getOid()
            location = segFilespaces[oid]
            filespaceNames.append(name)
            filespaceLocations.append(location)
        for i in range(len(filespaceNames)):
            entry = [filespaceNames[i], filespaceLocations[i]]
            filespaces.append(entry)
        return str(filespaces)

    def update_catalog(self):
        """
        Starts the database, calls updateSystemConfig() to setup
        the catalog tables and get the actual dbid and content id
        for the new segments.
        """
        self.statusLogger.set_gp_segment_configuration_backup(
            self.options.master_data_directory + '/' + SEGMENT_CONFIGURATION_BACKUP_FILE)
        self.gparray.dumpToFile(self.statusLogger.get_gp_segment_configuration_backup())
        self.statusLogger.set_status('UPDATE_CATALOG_STARTED', self.statusLogger.get_gp_segment_configuration_backup())

        self.logger.info('Starting Greenplum Database in restricted mode')
        startCmd = GpStart('gpexpand update master start database restricted mode', restricted=True, verbose=True)
        startCmd.run(validateAfter=True)

        # Put expansion segment primaries in change tracking
        for seg in self.gparray.getExpansionSegDbList():
            if seg.isSegmentMirror() == True:
                continue
            if self.gparray.get_mirroring_enabled() == True:
                seg.setSegmentMode(MODE_CHANGELOGGING)

        # Set expansion segment mirror state = down
        for seg in self.gparray.getExpansionSegDbList():
            if seg.isSegmentPrimary() == True:
                continue
            seg.setSegmentStatus(STATUS_DOWN)

        # Update the catalog
        configurationInterface.getConfigurationProvider().updateSystemConfig(
            self.gparray,
            "%s: segment config for resync" % getProgramName(),
            dbIdToForceMirrorRemoveAdd={},
            useUtilityMode=True,
            allowPrimary=True
        )

        # The content IDs may have changed, so we must make sure the array is in proper order.
        self.gparray.reOrderExpansionSegs()

        # Issue checkpoint due to forced shutdown below
        self.conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8')
        dbconn.execSQL(self.conn, "CHECKPOINT")
        self.conn.close()

        self.logger.info('Stopping database')
        stopCmd = GpStop('gpexpand update master stop database', verbose=True, ctxt=LOCAL, force=True)
        # We do not check the results of GpStop becuase we will get errors for all the new segments.
        stopCmd.run(validateAfter=False)

        self.statusLogger.set_status('UPDATE_CATALOG_DONE')

    # --------------------------------------------------------------------------
    def configure_new_segment_filespaces(self):
        """
        This method is called after all new segments have been configured.
        """

        self.logger.info('Configuring new segment filespaces')
        newSegments = self.gparray.getExpansionSegDbList()
        fsObjList = self.gparray.getFilespaces(includeSystemFilespace=False)

        if len(fsObjList) == 0:
            # No filespaces to configure
            return

        """ Connect to the back end of each new segment directly, and call the filespace setup function. """
        for seg in newSegments:
            if seg.isSegmentMirror() == True:
                continue
            name = "gpexpand prep new segment filespaces. host = %s, sysdatadir = %s" % (
                seg.getSegmentHostName(), seg.getSegmentDataDirectory())
            segFilespaces = seg.getSegmentFilespaces()
            filespaceNames = []
            filespaceLocations = []
            for entry in fsObjList:
                fsname = entry.getName()
                oid = entry.getOid()
                location = segFilespaces[oid]
                filespaceNames.append(fsname)
                filespaceLocations.append(location)
            prepCmd = PrepFileSpaces(name=name
                                     , filespaceNames=filespaceNames
                                     , filespaceLocations=filespaceLocations
                                     , sysDataDirectory=seg.getSegmentDataDirectory()
                                     , dbid=seg.getSegmentDbId()
                                     , contentId=seg.getSegmentContentId()
                                     , ctxt=REMOTE
                                     , remoteHost=seg.getSegmentHostName()
                                     )
            self.pool.addCommand(prepCmd)
        self.pool.join()
        self.pool.check_results()

    # --------------------------------------------------------------------------
    def cleanup_new_segments(self):
        """
        This method is called after all new segments have been configured.
        """

        self.logger.info('Cleaning up databases in new segments.')
        newSegments = self.gparray.getExpansionSegDbList()

        """ Get a list of databases. """
        self.logger.info('Starting master in utility mode')

        startCmd = GpStart('gpexpand update master start database master only', masterOnly=True)
        startCmd.run(validateAfter=True)

        conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8')
        databases = catalog.getDatabaseList(conn)
        conn.close()

        self.logger.info('Stopping master in utility mode')
        GpStop.local('gpexpand update master stop database', masterOnly=True, fast=True)

        """
        Connect to each database in each segment and do some cleanup of tables that have stuff in them as a result of copying the segment from the master.
        Note, this functionaliy used to be in segcopy and was therefore done just once to the original copy of the master. We need to do it separately for
        each segment now since filespaces may contain the databases.
        """
        for seg in newSegments:
            if seg.isSegmentMirror() == True:
                continue
            """ Start all the new segments in utilty mode. """
            segStartCmd = SegmentStart(
                name="Starting new segment dbid %s on host %s." % (str(seg.getSegmentDbId()), seg.getSegmentHostName())
                , gpdb=seg
                , numContentsInCluster=0  # Starting seg on it's own.
                , era=None
                , mirrormode=MIRROR_MODE_MIRRORLESS
                , utilityMode=True
                , ctxt=REMOTE
                , remoteHost=seg.getSegmentHostName()
                , noWait=False
                , timeout=SEGMENT_TIMEOUT_DEFAULT)
            self.pool.addCommand(segStartCmd)
        self.pool.join()
        self.pool.check_results()

        """
        Build the list of delete statements based on the MASTER_ONLY_TABLES
        defined in gpcatalog.py
        """
        statements = ["delete from pg_catalog.%s" % tab for tab in MASTER_ONLY_TABLES]

        """
          Connect to each database in the new segments, and clean up the catalog tables.
        """
        for seg in newSegments:
            if seg.isSegmentMirror() == True:
                continue
            for database in databases:
                if database[0] == 'template0':
                    continue
                dburl = dbconn.DbURL(hostname=seg.getSegmentHostName()
                                     , port=seg.getSegmentPort()
                                     , dbname=database[0]
                                     )
                name = "gpexpand execute segment cleanup commands. seg dbid = %s, command = %s" % (
                    seg.getSegmentDbId(), str(statements))
                execSQLCmd = ExecuteSQLStatementsCommand(name=name
                                                         , url=dburl
                                                         , sqlCommandList=statements
                                                         )
                self.pool.addCommand(execSQLCmd)
                self.pool.join()
                ### need to fix self.pool.check_results(). Call getCompletedItems to clear the queue for now.
                self.pool.check_results()
                self.pool.getCompletedItems()

        """
        Stop all the new segments.
        """
        for seg in newSegments:
            if seg.isSegmentMirror() == True:
                continue
            segStopCmd = SegmentStop(
                name="Stopping new segment dbid %s on host %s." % (str(seg.getSegmentDbId), seg.getSegmentHostName())
                , dataDir=seg.getSegmentDataDirectory()
                , mode='smart'
                , nowait=False
                , ctxt=REMOTE
                , remoteHost=seg.getSegmentHostName()
            )
            self.pool.addCommand(segStopCmd)
        self.pool.join()
        self.pool.check_results()

        self.logger.info('Starting Greenplum Database in restricted mode')
        startCmd = GpStart('gpexpand update master start database restricted', restricted=True, verbose=True)
        startCmd.run(validateAfter=True)

        # Need to restore the connection used by the expansion
        self.conn = dbconn.connect(self.dburl, encoding='UTF8')

    # --------------------------------------------------------------------------
    def restore_master(self):
        """Restores the gp_segment_configuration catalog table for rollback"""
        backupFile = self.statusLogger.get_gp_segment_configuration_backup()

        if not os.path.exists(backupFile):
            raise ExpansionError('gp_segment_configuration backup file %s does not exist' % backupFile)

        # Create a new gpArray from the backup file
        array = GpArray.initFromFile(backupFile)

        originalDbIds = ""
        originalDbIdsList = []
        first = True
        for seg in array.getDbList():
            originalDbIdsList.append(int(seg.getSegmentDbId()))
            if first == False:
                originalDbIds += ", "
            first = False
            originalDbIds += str(seg.getSegmentDbId())

        if len(originalDbIds) > 0:
            # Update the catalog with the contents of the backup
            restore_conn = None
            try:
                restore_conn = dbconn.connect(self.dburl, utility=True, encoding='UTF8', allowSystemTableMods='dml')

                # Get a list of all the expand primary segments
                sqlStr = "select dbid from pg_catalog.gp_segment_configuration where dbid not in (%s) and role = 'p'" % str(
                    originalDbIds)
                curs = dbconn.execSQL(restore_conn, sqlStr)
                deleteDbIdList = []
                rows = curs.fetchall()
                for row in rows:
                    deleteDbIdList.append(int(row[0]))

                # Get a list of all the expand mirror segments
                sqlStr = "select content from pg_catalog.gp_segment_configuration where dbid not in (%s) and role = 'm'" % str(
                    originalDbIds)
                curs = dbconn.execSQL(restore_conn, sqlStr)
                deleteContentList = []
                rows = curs.fetchall()
                for row in rows:
                    deleteContentList.append(int(row[0]))

                #
                # The following is a sanity check to make sure we don't do something bad here.
                #
                if len(originalDbIdsList) < 2:
                    self.logger.error(
                        "The original DB DIS list is to small to be correct: %s " % str(len(originalDbIdsList)))
                    raise Exception("Unable to complete rollback")

                totalToDelete = len(deleteDbIdList) + len(deleteContentList)
                if int(totalToDelete) > int(self.statusLogger.get_number_new_segments()):
                    self.logger.error(
                        "There was a discrepancy between the number of expand segments to rollback (%s), and the expected number of segment to rollback (%s)" \
                        % (str(totalToDelete), str(self.statusLogger.get_number_new_segments())))
                    self.logger.error("  Expanded primary segment dbids = %s", str(deleteDbIdList))
                    self.logger.error("  Expansion mirror content ids   = %s", str(deleteContentList))
                    raise Exception("Unable to complete rollback")

                for content in deleteContentList:
                    sqlStr = "select * from gp_remove_segment_mirror(%s::smallint)" % str(content)
                    dbconn.execSQL(restore_conn, sqlStr)

                for dbid in deleteDbIdList:
                    sqlStr = "select * from gp_remove_segment(%s::smallint)" % str(dbid)
                    dbconn.execSQL(restore_conn, sqlStr)

                restore_conn.commit()
            except Exception, e:
                raise Exception("Unable to restore master. Exception: " + str(e))
            finally:
                if restore_conn != None:
                    restore_conn.close()

    def sync_new_mirrors(self):
        """ This method will execute gprecoverseg so that all new segments sync with their mirrors."""
        if self.gparray.get_mirroring_enabled() == True:
            self.logger.info('Starting new mirror segment synchronization')
            cmd = GpRecoverSeg(name="gpexpand syncing mirrors", options="-a -F")
            cmd.run(validateAfter=True)

    def start_prepare(self):
        """Inserts into gpexpand.status that expansion preparation has started."""
        if self.options.filename:
            self.statusLogger.create_status_file()
            self.statusLogger.set_status('EXPANSION_PREPARE_STARTED', os.path.abspath(self.options.filename))

    def finalize_prepare(self):
        """Removes the gpexpand status file and segment configuration backup file"""
        self.statusLogger.remove_status_file()
        self.statusLogger.remove_segment_configuration_backup_file()
        self.pastThePointOfNoReturn = True;

    def setup_schema(self):
        """Used to setup the gpexpand schema"""
        self.statusLogger.set_status('SETUP_EXPANSION_SCHEMA_STARTED')
        self.logger.info('Creating expansion schema')
        dbconn.execSQL(self.conn, create_schema_sql)
        dbconn.execSQL(self.conn, status_table_sql)
        dbconn.execSQL(self.conn, status_detail_table_sql)

        # views
        if not self.options.simple_progress:
            dbconn.execSQL(self.conn, progress_view_sql)
        else:
            dbconn.execSQL(self.conn, progress_view_simple_sql)

        self.conn.commit()

        self.statusLogger.set_status('SETUP_EXPANSION_SCHEMA_DONE')

    def prepare_schema(self):
        """Prepares the gpexpand schema"""
        self.statusLogger.set_status('PREPARE_EXPANSION_SCHEMA_STARTED')

        if not self.conn:
            self.conn = dbconn.connect(self.dburl, encoding='UTF8', allowSystemTableMods='dml')
            self.gparray = GpArray.initFromCatalog(self.dburl)

        nowStr = datetime.datetime.now()
        statusSQL = "INSERT INTO %s.%s VALUES ( 'SETUP', '%s' ) " % (gpexpand_schema, status_table, nowStr)

        dbconn.execSQL(self.conn, statusSQL)

        db_list = catalog.getDatabaseList(self.conn)

        for db in db_list:
            dbname = db[0]
            if dbname == 'template0':
                continue
            self.logger.info('Populating %s.%s with data from database %s' % (
                gpexpand_schema, status_detail_table, dbname.decode('utf-8')))
            self._populate_regular_tables(dbname)
            self._populate_partitioned_tables(dbname)
            inject_fault('gpexpand MPP-14620 fault injection')
            self._update_distribution_policy(dbname)

        nowStr = datetime.datetime.now()
        statusSQL = "INSERT INTO %s.%s VALUES ( 'SETUP DONE', '%s' ) " % (gpexpand_schema, status_table, nowStr)
        dbconn.execSQL(self.conn, statusSQL)

        self.conn.commit()

        self.conn.close()

        self.statusLogger.set_status('PREPARE_EXPANSION_SCHEMA_DONE')
        self.statusLogger.set_status('EXPANSION_PREPARE_DONE')

        # At this point, no rollback is possible and the the system
        # including new segments has been started once before so finalize
        self.finalize_prepare()

        self.logger.info('Stopping Greenplum Database')
        GpStop.local('gpexpand setup complete', fast=True)

    def _populate_regular_tables(self, dbname):
        """ we don't do 3.2+ style partitioned tables here, but we do
            all other table types.
        """

        src_bytes_str = "0" if self.options.simple_progress else "pg_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))"
        sql = """SELECT
    n.nspname || '.' || c.relname as fq_name,
    n.oid as schemaoid,
    c.oid as tableoid,
    p.attrnums as distribution_policy,
    now() as last_updated,
    %s
FROM
            pg_class c
    JOIN pg_namespace n ON (c.relnamespace=n.oid)
    JOIN pg_catalog.gp_distribution_policy p on (c.oid = p.localoid)
    LEFT JOIN pg_partition pp on (c.oid=pp.parrelid)
    LEFT JOIN pg_partition_rule pr on (c.oid=pr.parchildrelid)
WHERE
    pp.parrelid is NULL
    AND pr.parchildrelid is NULL
    AND n.nspname != 'gpexpand'
    AND n.nspname != 'pg_bitmapindex'
    AND c.relstorage != 'x';

                  """ % (src_bytes_str)
        self.logger.debug(sql)
        table_conn = self.connect_database(dbname)
        curs = dbconn.execSQL(table_conn, sql)
        rows = curs.fetchall()
        try:
            sql_file = os.path.abspath('./%s.dat' % status_detail_table)
            self.logger.debug('status_detail data file: %s' % sql_file)
            fp = open(sql_file, 'w')
            for row in rows:
                fqname = row[0]
                schema_oid = row[1]
                table_oid = row[2]
                if row[3]:
                    self.logger.debug("dist policy raw: %s " % row[3].decode('utf-8'))
                else:
                    self.logger.debug("dist policy raw: NULL")
                dist_policy = row[3]
                (policy_name, policy_oids) = self.form_dist_policy_name(table_conn, row[3], table_oid)
                rel_bytes = int(row[5])

                if dist_policy is None:
                    dist_policy = 'NULL'

                full_name = '%s.%s' % (dbname, fqname)
                rank = 1 if self.unique_index_tables.has_key(full_name) else 2

                fp.write("""%s\t%s\t%s\t%s\t%s\t%s\t%s\tNULL\t%d\t%s\tNULL\tNULL\t%d\n""" % (
                    dbname, fqname, schema_oid, table_oid,
                    dist_policy, policy_name, policy_oids,
                    rank, undone_status, rel_bytes))
        except Exception, e:
            raise ExpansionError(e)
        finally:
            if fp: fp.close()

        try:
            copySQL = """COPY %s.%s FROM '%s' NULL AS 'NULL'""" % (gpexpand_schema, status_detail_table, sql_file)

            self.logger.debug(copySQL)
            dbconn.execSQL(self.conn, copySQL)
        except Exception, e:
            raise ExpansionError(e)
        finally:
            os.unlink(sql_file)

        table_conn.commit()
        table_conn.close()

    def _populate_partitioned_tables(self, dbname):
        """population of status_detail for partitioned tables. """
        src_bytes_str = "0" if self.options.simple_progress else "pg_relation_size(quote_ident(p.partitionschemaname) || '.' || quote_ident(p.partitiontablename))"
        sql = """
SELECT
    p.partitionschemaname || '.' || p.partitiontablename as fq_name,
    n.oid as schemaoid,
    c2.oid as tableoid,
    d.attrnums as distributed_policy,
    now() as last_updated,
    %s,
    partitiontype,partitionlevel,partitionrank,partitionposition,
    partitionrangestart
FROM
    pg_partitions p,
    pg_class c,
    pg_class c2,
    pg_namespace n,
    pg_namespace n2,
    gp_distribution_policy d
WHERE
    quote_ident(p.tablename) = quote_ident(c.relname)
    AND    d.localoid = c2.oid
    AND quote_ident(p.schemaname) = quote_ident(n.nspname)
    AND c.relnamespace = n.oid
    AND p.partitionlevel = (select max(parlevel) FROM pg_partition WHERE parrelid = c.oid)
    AND quote_ident(p.partitionschemaname) = quote_ident(n2.nspname)
    AND quote_ident(p.partitiontablename) = quote_ident(c2.relname)
    AND c2.relnamespace = n2.oid
    AND c2.relstorage != 'x'
ORDER BY tablename, c2.oid desc;
                  """ % (src_bytes_str)
        self.logger.debug(sql)
        table_conn = self.connect_database(dbname)
        curs = dbconn.execSQL(table_conn, sql)
        rows = curs.fetchall()

        try:
            sql_file = os.path.abspath('./%s.dat' % status_detail_table)
            self.logger.debug('status_detail data file: %s' % sql_file)
            fp = open(sql_file, 'w')

            for row in rows:
                fqname = row[0]
                schema_oid = row[1]
                table_oid = row[2]
                if row[3]:
                    self.logger.debug("dist policy raw: %s " % row[3])
                else:
                    self.logger.debug("dist policy raw: NULL")
                dist_policy = row[3]
                (policy_name, policy_oids) = self.form_dist_policy_name(table_conn, row[3], table_oid)
                rel_bytes = int(row[5])

                if dist_policy is None:
                    dist_policy = 'NULL'

                full_name = '%s.%s' % (dbname, fqname)
                rank = 1 if self.unique_index_tables.has_key(full_name) else 2

                fp.write("""%s\t%s\t%s\t%s\t%s\t%s\t%s\tNULL\t%d\t%s\tNULL\tNULL\t%d\n""" % (
                    dbname, fqname, schema_oid, table_oid,
                    dist_policy, policy_name, policy_oids,
                    rank, undone_status, rel_bytes))
        except Exception:
            raise
        finally:
            if fp: fp.close()

        try:
            copySQL = """COPY %s.%s FROM '%s' NULL AS 'NULL'""" % (gpexpand_schema, status_detail_table, sql_file)

            self.logger.debug(copySQL)
            dbconn.execSQL(self.conn, copySQL)
        except Exception, e:
            raise ExpansionError(e)
        finally:
            os.unlink(sql_file)

        table_conn.commit()
        table_conn.close()

    def _update_distribution_policy(self, dbname):
        """ NULL out the distribution policy for both
            regular and paritioned table before expansion
        """

        table_conn = self.connect_database(dbname)
        # null out the dist policies
        sql = """
UPDATE  gp_distribution_policy
  SET attrnums = NULL
FROM pg_class c
    JOIN pg_namespace n ON (c.relnamespace=n.oid)
    LEFT JOIN pg_partition pp ON (c.oid=pp.parrelid)
    LEFT JOIN pg_partition_rule pr ON (c.oid=pr.parchildrelid)
WHERE
    localoid = c.oid
    AND pp.parrelid IS NULL
    AND pr.parchildrelid IS NULL
    AND n.nspname != 'gpexpand';
        """

        self.logger.debug(sql)
        dbconn.execSQL(table_conn, sql)

        sql = """
UPDATE gp_distribution_policy
    SET attrnums = NULL
    FROM
        ( SELECT pp.parrelid AS tableoid,
                 n2.nspname AS partitionschemaname, cl2.relname AS partitiontablename,
                 cl2.oid AS partitiontableoid, pr1.parname AS partitionname, cl3.relname AS parentpartitiontablename, pr2.parname AS parentpartitioname,
                    pp.parlevel AS partitionlevel, pr1.parruleord AS partitionposition
               FROM pg_namespace n, pg_namespace n2, pg_class cl, pg_class cl2, pg_partition pp, pg_partition_rule pr1
          LEFT JOIN pg_partition_rule pr2 ON pr1.parparentrule = pr2.oid
       LEFT JOIN pg_class cl3 ON pr2.parchildrelid = cl3.oid
      WHERE pp.paristemplate = FALSE AND pp.parrelid = cl.oid AND pr1.paroid = pp.oid AND cl2.oid = pr1.parchildrelid AND cl.relnamespace = n.oid AND cl2.relnamespace = n2.oid
    ) p1
    WHERE
    localoid = p1.partitiontableoid
    AND p1.partitionlevel = (SELECT max(parlevel) FROM pg_partition WHERE parrelid = p1.tableoid);

"""
        self.logger.debug(sql)
        dbconn.execSQL(table_conn, sql)
        table_conn.commit()
        table_conn.close()

    def form_dist_policy_name(self, conn, rs_val, table_oid):
        if rs_val is None:
            return (None, None)
        rs_val = rs_val.lstrip('{').rstrip('}').strip()

        namedict = {}
        oiddict = {}
        sql = "select attnum, attname, attrelid from pg_attribute where attrelid =  %s and attnum > 0" % table_oid
        cursor = dbconn.execSQL(conn, sql)
        for row in cursor:
            namedict[row[0]] = row[1]
            oiddict[row[0]] = row[2]

        name_list = []
        oid_list = []

        if rs_val != "":
            rs_list = rs_val.split(',')

            for colnum in rs_list:
                name_list.append(namedict[int(colnum)])
                oid_list.append(str(oiddict[int(colnum)]))

        return (' , '.join(name_list), ' , '.join(oid_list))

    def perform_expansion(self):
        """Performs the actual table re-organiations"""
        expansionStart = datetime.datetime.now()

        # setup a threadpool
        self.queue = WorkerPool(numWorkers=self.numworkers)

        # go through and reset any "IN PROGRESS" tables
        self.conn = dbconn.connect(self.dburl, encoding='UTF8')
        sql = "INSERT INTO %s.%s VALUES ( 'EXPANSION STARTED', '%s' ) " % (
            gpexpand_schema, status_table, expansionStart)
        cursor = dbconn.execSQL(self.conn, sql)
        self.conn.commit()

        sql = """UPDATE gpexpand.status_detail set status = '%s' WHERE status = '%s' """ % (undone_status, start_status)
        cursor = dbconn.execSQL(self.conn, sql)
        self.conn.commit()

        # read schema and queue up commands
        sql = "SELECT * FROM %s.%s WHERE status = 'NOT STARTED' ORDER BY rank" % (gpexpand_schema, status_detail_table)
        cursor = dbconn.execSQL(self.conn, sql)

        for row in cursor:
            self.logger.debug(row)
            name = "name"
            tbl = ExpandTable(options=self.options, row=row)
            cmd = ExpandCommand(name=name, status_url=self.dburl, table=tbl, options=self.options)
            self.queue.addCommand(cmd)

        table_expand_error = False

        stopTime = None
        stoppedEarly = False
        if self.options.end:
            stopTime = self.options.end

        # wait till done.
        while not self.queue.isDone():
            logger.debug(
                "woke up.  queue: %d finished %d  " % (self.queue.num_assigned, self.queue.completed_queue.qsize()))
            if stopTime and datetime.datetime.now() >= stopTime:
                stoppedEarly = True
                break
            time.sleep(5)

        expansionStopped = datetime.datetime.now()

        self.pool.haltWork()
        self.pool.joinWorkers()
        self.queue.haltWork()
        self.queue.joinWorkers()

        # Doing this after the halt and join workers guarantees that no new completed items can be added
        # while we're doing a check
        for expandCommand in self.queue.getCompletedItems():
            if expandCommand.table_expand_error:
                table_expand_error = True
                break

        if stoppedEarly:
            logger.info('End time reached.  Stopping expansion.')
            sql = "INSERT INTO %s.%s VALUES ( 'EXPANSION STOPPED', '%s' ) " % (
                gpexpand_schema, status_table, expansionStopped)
            cursor = dbconn.execSQL(self.conn, sql)
            self.conn.commit()
            logger.info('You can resume expansion by running gpexpand again')
        elif table_expand_error:
            logger.warn('**************************************************')
            logger.warn('One or more tables failed to expand successfully.')
            logger.warn('Please check the log file, correct the problem and')
            logger.warn('run gpexpand again to finish the expansion process')
            logger.warn('**************************************************')
            # We'll try to update the status, but if the errors were caused by
            # going into read only mode, this will fail.  That's ok though as
            # gpexpand will resume next run
            try:
                sql = "INSERT INTO %s.%s VALUES ( 'EXPANSION STOPPED', '%s' ) " % (
                    gpexpand_schema, status_table, expansionStopped)
                cursor = dbconn.execSQL(self.conn, sql)
                self.conn.commit()
            except:
                pass
        else:
            sql = "INSERT INTO %s.%s VALUES ( 'EXPANSION COMPLETE', '%s' ) " % (
                gpexpand_schema, status_table, expansionStopped)
            cursor = dbconn.execSQL(self.conn, sql)
            self.conn.commit()
            logger.info("EXPANSION COMPLETED SUCCESSFULLY")

    def shutdown(self):
        """used if the script is closed abrubtly"""
        logger.info('Shutting down gpexpand...')
        if self.pool:
            self.pool.haltWork()
            self.pool.joinWorkers()

        if self.queue:
            self.queue.haltWork()
            self.queue.joinWorkers()

        try:
            expansionStopped = datetime.datetime.now()
            sql = "INSERT INTO %s.%s VALUES ( 'EXPANSION STOPPED', '%s' ) " % (
                gpexpand_schema, status_table, expansionStopped)
            cursor = dbconn.execSQL(self.conn, sql)
            self.conn.commit()

            cursor.close()
            self.conn.close()
        except pg.OperationalError:
            pass
        except Exception:
            # schema doesn't exist.  Cancel or error during setup
            pass

    def halt_work(self):
        if self.pool:
            self.pool.haltWork()
            self.pool.joinWorkers()

        if self.queue:
            self.queue.haltWork()
            self.queue.joinWorkers()

    def cleanup_schema(self, gpexpand_db_status):
        """Removes the gpexpand schema"""
        # drop schema
        if gpexpand_db_status != 'EXPANSION COMPLETE':
            c = dbconn.connect(self.dburl, encoding='UTF8')
            self.logger.warn('Expansion has not yet completed.  Removing the expansion')
            self.logger.warn('schema now will leave the following tables unexpanded:')
            unexpanded_tables_sql = "SELECT fq_name FROM %s.%s WHERE status = 'NOT STARTED' ORDER BY rank" % (
                gpexpand_schema, status_detail_table)

            cursor = dbconn.execSQL(c, unexpanded_tables_sql)
            unexpanded_tables_text = ''.join("\t%s\n" % row[0] for row in cursor)

            c.close()

            self.logger.warn(unexpanded_tables_text)
            self.logger.warn('These tables will have to be expanded manually by setting')
            self.logger.warn('the distribution policy using the ALTER TABLE command.')
            if not ask_yesno('', "Are you sure you want to drop the expansion schema?", 'N'):
                logger.info("User Aborted. Exiting...")
                sys.exit(0)

        # See if user wants to dump the status_detail table to file
        c = dbconn.connect(self.dburl, encoding='UTF8')
        if ask_yesno('', "Do you want to dump the gpexpand.status_detail table to file?", 'Y'):
            self.logger.info(
                "Dumping gpexpand.status_detail to %s/gpexpand.status_detail" % self.options.master_data_directory)
            copy_gpexpand_status_detail_sql = "COPY gpexpand.status_detail TO '%s/gpexpand.status_detail'" % self.options.master_data_directory
            dbconn.execSQL(c, copy_gpexpand_status_detail_sql)

        self.logger.info("Removing gpexpand schema")
        dbconn.execSQL(c, drop_schema_sql)
        c.commit()
        c.close()

    def connect_database(self, dbname):
        test_url = copy.deepcopy(self.dburl)
        test_url.pgdb = dbname
        c = dbconn.connect(test_url, encoding='UTF8', allowSystemTableMods='dml')
        return c

    def sync_packages(self):
        """
        The design decision here is to squash any exceptions resulting from the
        synchronization of packages. We should *not* disturb the user's attempts to expand.
        """
        try:
            logger.info('Syncing Greenplum Database extensions')
            new_segment_list = self.gparray.getExpansionSegDbList()
            new_host_set = set([h.getSegmentHostName() for h in new_segment_list])
            operations = [SyncPackages(host) for host in new_host_set]
            ParallelOperation(operations, self.numworkers).run()
            # introspect outcomes
            for operation in operations:
                operation.get_ret()
        except Exception:
            logger.exception('Syncing of Greenplum Database extensions has failed.')
            logger.warning('Please run gppkg --clean after successful expansion.')

    def move_filespaces(self):
        """
            Move filespaces for temporary and transaction files.
        """

        segments = self.gparray.getExpansionSegDbList()

        cur_filespace_entries = GetFilespaceEntriesDict(GetFilespaceEntries(self.gparray,
                                                                            PG_SYSTEM_FILESPACE
                                                                            ).run()).run()
        pg_system_filespace_entries = cur_filespace_entries
        cur_filespace_name = self.gparray.getFileSpaceName(int(cur_filespace_entries[1][0]))
        segments = self.gparray.getExpansionSegDbList()

        logger.info('Checking if Transaction filespace was moved')
        if os.path.exists(os.path.join(cur_filespace_entries[1][2], GP_TRANSACTION_FILES_FILESPACE)):
            logger.info('Transaction filespace was moved')
            new_filespace_entries = GetFilespaceEntriesDict(GetCurrentFilespaceEntries(self.gparray,
                                                                                       FileType.TRANSACTION_FILES
                                                                                       ).run()).run()
            new_filespace_name = self.gparray.getFileSpaceName(int(new_filespace_entries[1][0]))
            operations_list = GetMoveOperationList(segments,
                                                   FileType.TRANSACTION_FILES,
                                                   new_filespace_name,
                                                   new_filespace_entries,
                                                   cur_filespace_entries,
                                                   pg_system_filespace_entries
                                                   ).run()

            logger.info('Moving Transaction filespace on expansion segments')
            ParallelOperation(operations_list).run()

            logger.debug('Checking results of transaction files move')
            for operation in operations_list:
                try:
                    operation.get_ret()
                except Exception as _:
                    logger.info('Transaction filespace move failed on expansion segment')
                    RollBackFilespaceChanges(segments,
                                             FileType.TRANSACTION_FILES,
                                             cur_filespace_name,
                                             cur_filespace_entries,
                                             new_filespace_entries,
                                             pg_system_filespace_entries,
                                             ).run()
                    raise

        logger.info('Checking if Temporary filespace was moved')
        if os.path.exists(os.path.join(cur_filespace_entries[1][2], GP_TEMPORARY_FILES_FILESPACE)):
            logger.info('Temporary filespace was moved')
            new_filespace_entries = GetFilespaceEntriesDict(GetCurrentFilespaceEntries(self.gparray,
                                                                                       FileType.TEMPORARY_FILES
                                                                                       ).run()).run()
            new_filespace_name = self.gparray.getFileSpaceName(int(new_filespace_entries[1][0]))
            operations_list = GetMoveOperationList(segments,
                                                   FileType.TEMPORARY_FILES,
                                                   new_filespace_name,
                                                   new_filespace_entries,
                                                   cur_filespace_entries,
                                                   pg_system_filespace_entries
                                                   ).run()

            logger.info('Moving Temporary filespace on expansion segments')
            ParallelOperation(operations_list).run()

            logger.debug('Checking results of temporary files move')
            for operation in operations_list:
                try:
                    operation.get_ret()
                except Exception:
                    logger.info('Temporary filespace move failed on expansion segment')
                    RollBackFilespaceChanges(segments,
                                             FileType.TEMPORARY_FILES,
                                             cur_filespace_name,
                                             cur_filespace_entries,
                                             new_filespace_entries,
                                             pg_system_filespace_entries
                                             ).run()
                    raise

        # Update flat files on mirrors
        UpdateFlatFiles(self.gparray, primaries=False, expansion=True).run()


    def validate_heap_checksums(self):
        num_workers = min(len(self.gparray.get_hostlist()), MAX_PARALLEL_EXPANDS)
        heap_checksum_util = HeapChecksum(gparray=self.gparray, num_workers=num_workers, logger=self.logger)
        successes, failures = heap_checksum_util.get_segments_checksum_settings()
        if len(successes) == 0:
            logger.fatal("No segments responded to ssh query for heap checksum. Not expanding the cluster.")
            return 1

        consistent, inconsistent, master_heap_checksum = heap_checksum_util.check_segment_consistency(successes)

        inconsistent_segment_msgs = []
        for segment in inconsistent:
            inconsistent_segment_msgs.append("dbid: %s "
                                             "checksum set to %s differs from master checksum set to %s" %
                                             (segment.getSegmentDbId(), segment.heap_checksum,
                                              master_heap_checksum))

        if not heap_checksum_util.are_segments_consistent(consistent, inconsistent):
            self.logger.fatal("Cluster heap checksum setting differences reported")
            self.logger.fatal("Heap checksum settings on %d of %d segment instances do not match master <<<<<<<<"
                              % (len(inconsistent_segment_msgs), len(self.gparray.segments)))
            self.logger.fatal("Review %s for details" % get_logfile())
            log_to_file_only("Failed checksum consistency validation:", logging.WARN)
            self.logger.fatal("gpexpand error: Cluster will not be modified as checksum settings are not consistent "
                              "across the cluster.")

            for msg in inconsistent_segment_msgs:
                log_to_file_only(msg, logging.WARN)
                raise Exception("Segments have heap_checksum set inconsistently to master")
        else:
            self.logger.info("Heap checksum setting consistent across cluster")


# -----------------------------------------------
class ExpandTable():
    def __init__(self, options, row=None):
        self.options = options
        if row is not None:
            (self.dbname, self.fq_name, self.schema_oid, self.table_oid,
             self.distrib_policy, self.distrib_policy_names, self.distrib_policy_coloids,
             self.storage_options, self.rank, self.status,
             self.expansion_started, self.expansion_finished,
             self.source_bytes) = row

    def add_table(self, conn):
        insertSQL = """INSERT INTO %s.%s
                            VALUES ('%s','%s',%s,%s,
                                    '%s','%s','%s','%s',%d,'%s','%s','%s',%d)
                    """ % (gpexpand_schema, status_detail_table,
                           self.dbname, self.fq_name, self.schema_oid, self.table_oid,
                           self.distrib_policy, self.distrib_policy_names, self.distrib_policy_coloids,
                           self.storage_options, self.rank, self.status,
                           self.expansion_started, self.expansion_finished,
                           self.source_bytes)
        logger.info('Added table %s.%s' % (self.dbname.decode('utf-8'), self.fq_name.decode('utf-8')))
        logger.debug(insertSQL.decode('utf-8'))
        dbconn.execSQL(conn, insertSQL)

    def mark_started(self, status_conn, table_conn, start_time, cancel_flag):
        if cancel_flag:
            return
        (schema_name, table_name) = self.fq_name.split('.')
        sql = "SELECT pg_relation_size(quote_ident('%s') || '.' || quote_ident('%s'))" % (schema_name, table_name)
        cursor = dbconn.execSQL(table_conn, sql)
        row = cursor.fetchone()
        src_bytes = int(row[0])
        logger.debug(" Table: %s has %d bytes" % (self.fq_name.decode('utf-8'), src_bytes))

        sql = """UPDATE %s.%s
                  SET status = '%s', expansion_started='%s',
                      source_bytes = %d
                  WHERE dbname = '%s' AND schema_oid = %s
                        AND table_oid = %s """ % (gpexpand_schema, status_detail_table,
                                                  start_status, start_time,
                                                  src_bytes, self.dbname,
                                                  self.schema_oid, self.table_oid)

        logger.debug("Mark Started: " + sql.decode('utf-8'))
        dbconn.execSQL(status_conn, sql)
        status_conn.commit()

    def reset_started(self, status_conn):
        sql = """UPDATE %s.%s
                 SET status = '%s', expansion_started=NULL, expansion_finished=NULL
                 WHERE dbname = '%s' AND schema_oid = %s
                 AND table_oid = %s """ % (gpexpand_schema, status_detail_table, undone_status,
                                           self.dbname, self.schema_oid, self.table_oid)

        logger.debug('Reseting detailed_status: %s' % sql.decode('utf-8'))
        dbconn.execSQL(status_conn, sql)
        status_conn.commit()

    def expand(self, table_conn, cancel_flag):
        foo = self.distrib_policy_names.strip()
        new_storage_options = ''
        if self.storage_options:
            new_storage_options = ',' + self.storage_options

        (schema_name, table_name) = self.fq_name.split('.')

        logger.info("Distribution policy for table %s is '%s' " % (self.fq_name.decode('utf-8'), foo.decode('utf-8')))
        # logger.info("Storage options for table %s is %s" % (self.fq_name, self.storage_options))

        if foo == "" or foo == "None" or foo is None:
            sql = 'ALTER TABLE ONLY "%s"."%s" SET WITH(REORGANIZE=TRUE%s) DISTRIBUTED RANDOMLY' % (
                schema_name, table_name, new_storage_options)
        else:
            dist_cols = foo.split(',')
            dist_cols = ['"%s"' % x.strip() for x in dist_cols]
            dist_cols = ','.join(dist_cols)
            sql = 'ALTER TABLE ONLY "%s"."%s" SET WITH(REORGANIZE=TRUE%s) DISTRIBUTED BY (%s)' % (
                schema_name, table_name, new_storage_options, dist_cols)

        logger.info('Expanding %s.%s' % (self.dbname.decode('utf-8'), self.fq_name.decode('utf-8')))
        logger.debug("Expand SQL: %s" % sql.decode('utf-8'))

        # check is atomic in python
        if not cancel_flag:
            dbconn.execSQL(table_conn, sql)
            table_conn.commit()
            if self.options.analyze:
                sql = 'ANALYZE "%s"."%s"' % (schema_name, table_name)
                logger.info('Analyzing %s.%s' % (schema_name.decode('utf-8'), table_name.decode('utf-8')))
                dbconn.execSQL(table_conn, sql)
                table_conn.commit()

            return True

        # I can only get here if the cancel flag is True
        return False

    def mark_finished(self, status_conn, start_time, finish_time):
        sql = """UPDATE %s.%s
                  SET status = '%s', expansion_started='%s', expansion_finished='%s'
                  WHERE dbname = '%s' AND schema_oid = %s
                  AND table_oid = %s """ % (gpexpand_schema, status_detail_table,
                                            done_status, start_time, finish_time,
                                            self.dbname, self.schema_oid, self.table_oid)
        logger.debug(sql.decode('utf-8'))
        dbconn.execSQL(status_conn, sql)
        status_conn.commit()

    def mark_does_not_exist(self, status_conn, finish_time):
        sql = """UPDATE %s.%s
                  SET status = '%s', expansion_finished='%s'
                  WHERE dbname = '%s' AND schema_oid = %s
                  AND table_oid = %s """ % (gpexpand_schema, status_detail_table,
                                            does_not_exist_status, finish_time,
                                            self.dbname, self.schema_oid, self.table_oid)
        logger.debug(sql.decode('utf-8'))
        dbconn.execSQL(status_conn, sql)
        status_conn.commit()


# -----------------------------------------------
class PrepFileSpaces(Command):
    """
    This class will connect to a segment backend and execute the gp_prep_new_segment function to setup the file spaces.
    """

    def __init__(self, name, filespaceNames, filespaceLocations, sysDataDirectory, dbid, contentId, ctxt=LOCAL,
                 remoteHost=None):
        self.name = name
        self.filespaceNames = filespaceNames
        self.filespaceLocations = filespaceLocations
        self.sysDataDirectory = sysDataDirectory
        self.dbid = dbid
        self.contentId = contentId
        self.filespaces = []
        for i in range(len(filespaceNames)):
            entry = [filespaceNames[i], filespaceLocations[i]]
            self.filespaces.append(entry)
        cmdStr = """echo "select * from gp_prep_new_segment( array %s )" """ % (str(self.filespaces))
        cmdStr += """ | $GPHOME/bin/postgres --single --gp_num_contents_in_cluster=1 -O -c gp_session_role=utility -c gp_debug_linger=0 -c gp_before_filespace_setup=true  -E -D %s --gp_dbid=%s --gp_contentid=%s template1""" % (
            self.sysDataDirectory, str(self.dbid), str(self.contentId))
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)


# -----------------------------------------------
class ExecuteSQLStatementsCommand(SQLCommand):
    """
    This class will execute a list of SQL statements.
    """

    def __init__(self, name, url, sqlCommandList):
        self.name = name
        self.url = url
        self.sqlCommandList = sqlCommandList
        self.conn = None
        self.error = None

        SQLCommand.__init__(self, name)
        pass

    def run(self, validateAfter=False):
        statement = None

        faultPoint = os.getenv('GP_COMMAND_FAULT_POINT')
        if faultPoint and self.name and self.name.startswith(faultPoint):
            # simulate error
            self.results = CommandResult(1, 'Fault Injection', 'Fault Injection', False, True)
            self.error = "Fault Injection"
            return

        self.results = CommandResult(rc=0
                                     , stdout=""
                                     , stderr=""
                                     , completed=True
                                     , halt=False
                                     )

        try:
            self.conn = dbconn.connect(self.url, utility=True, encoding='UTF8', allowSystemTableMods='dml')
            for statement in self.sqlCommandList:
                dbconn.execSQL(self.conn, statement)
            self.conn.commit()
        except Exception, e:
            # traceback.print_exc()
            logger.error("Exception in ExecuteSQLStatements. URL = %s" % str(self.url))
            logger.error("  Statement = %s" % str(statement))
            logger.error("  Exception = %s" % str(e))
            self.error = str(e)
            self.results = CommandResult(rc=1
                                         , stdout=""
                                         , stderr=str(e)
                                         , completed=False
                                         , halt=True
                                         )
        finally:
            if self.conn != None:
                self.conn.close()

    def set_results(self, results):
        raise ExecutionError("TODO:  must implement", None)

    def get_results(self):
        return self.results

    def was_successful(self):
        if self.error != None:
            return False
        else:
            return True

    def validate(self, expected_rc=0):
        raise ExecutionError("TODO:  must implement", None)


# -----------------------------------------------
class ExpandCommand(SQLCommand):
    def __init__(self, name, status_url, table, options):
        self.status_url = status_url
        self.table = table
        self.options = options
        self.cmdStr = "Expand %s.%s" % (table.dbname, table.fq_name)
        self.table_url = copy.deepcopy(status_url)
        self.table_url.pgdb = table.dbname
        self.table_expand_error = False

        SQLCommand.__init__(self, name)
        pass

    def run(self, validateAfter=False):
        # connect.
        status_conn = None
        table_conn = None
        table_exp_success = False

        try:
            status_conn = dbconn.connect(self.status_url, encoding='UTF8')
            table_conn = dbconn.connect(self.table_url, encoding='UTF8')
        except DatabaseError, ex:
            if self.options.verbose:
                logger.exception(ex)
            logger.error(ex.__str__().strip())
            if status_conn: status_conn.close()
            if table_conn: table_conn.close()
            self.table_expand_error = True
            return

        # validate table hasn't been dropped
        start_time = None
        try:
            (schema_name, table_name) = self.table.fq_name.split('.')
            sql = """select * from pg_class c, pg_namespace n
            where c.relname = '%s' and n.oid = c.relnamespace and n.nspname='%s'""" % (table_name, schema_name)

            cursor = dbconn.execSQL(table_conn, sql)

            if cursor.rowcount == 0:
                logger.info('%s.%s no longer exists in database %s' % (schema_name.decode('utf-8'),
                                                                       table_name.decode('utf-8'),
                                                                       self.table.dbname.decode('utf-8')))

                self.table.mark_does_not_exist(status_conn, datetime.datetime.now())
                status_conn.close()
                table_conn.close()
                return
            else:
                # Set conn for  cancel
                self.cancel_conn = table_conn
                start_time = datetime.datetime.now()
                if not self.options.simple_progress:
                    self.table.mark_started(status_conn, table_conn, start_time, self.cancel_flag)

                table_exp_success = self.table.expand(table_conn, self.cancel_flag)

        except Exception, ex:
            if ex.__str__().find('canceling statement due to user request') == -1 and not self.cancel_flag:
                self.table_expand_error = True
                if self.options.verbose:
                    logger.exception(ex)
                logger.error('Table %s.%s failed to expand: %s' % (self.table.dbname.decode('utf-8'),
                                                                   self.table.fq_name.decode('utf-8'),
                                                                   ex.__str__().strip()))
            else:
                logger.info('ALTER TABLE of %s.%s canceled' % (
                    self.table.dbname.decode('utf-8'), self.table.fq_name.decode('utf-8')))

        if table_exp_success:
            end_time = datetime.datetime.now()
            # update metadata
            logger.info(
                "Finished expanding %s.%s" % (self.table.dbname.decode('utf-8'), self.table.fq_name.decode('utf-8')))
            self.table.mark_finished(status_conn, start_time, end_time)
        elif not self.options.simple_progress:
            logger.info("Reseting status_detail for %s.%s" % (
                self.table.dbname.decode('utf-8'), self.table.fq_name.decode('utf-8')))
            self.table.reset_started(status_conn)

        # disconnect
        status_conn.close()
        table_conn.close()

    def set_results(self, results):
        raise ExecutionError("TODO:  must implement", None)

    def get_results(self):
        raise ExecutionError("TODO:  must implement", None)

    def was_successful(self):
        raise ExecutionError("TODO:  must implement", None)

    def validate(self, expected_rc=0):
        raise ExecutionError("TODO:  must implement", None)


# ------------------------------- UI Help --------------------------------
def read_hosts_file(hosts_file):
    new_hosts = []
    try:
        f = open(hosts_file, 'r')
        try:
            for l in f:
                if l.strip().startswith('#') or l.strip() == '':
                    continue

                new_hosts.append(l.strip())

        finally:
            f.close()
    except IOError:
        raise ExpansionError('Hosts file %s not found' % hosts_file)

    return new_hosts


def interview_setup(gparray, options):
    help = """
System Expansion is used to add segments to an existing GPDB array.
gpexpand did not detect a System Expansion that is in progress.

Before initiating a System Expansion, you need to provision and burn-in
the new hardware.  Please be sure to run gpcheckperf to make sure the
new hardware is working properly.

Please refer to the Admin Guide for more information."""

    if not ask_yesno(help, "Would you like to initiate a new System Expansion", 'N'):
        logger.info("User Aborted. Exiting...")
        sys.exit(0)

    help = """
This utility can handle some expansion scenarios by asking a few questions.
More complex expansions can be done by providing an input file with
the --input <file>.  Please see the docs for the format of this file. """

    standard, message = gparray.isStandardArray()
    if standard == False:
        help = help + """

       The current system appears to be non-standard.
       """
        help = help + message
        help = help + """
       gpexpand may not be able to symmetrically distribute the new segments appropriately.
       It is recommended that you specify your own input file with appropriate values."""
        if not ask_yesno(help, "Are you sure you want to continue with this gpexpand session?", 'N'):
            logger.info("User Aborted. Exiting...")
            sys.exit(0)

    help = help + """

We'll now ask you a few questions to try and build this file for you.
You'll have the opportunity to save this file and inspect it/modify it
before continuing by re-running this utility and providing the input file. """

    def datadir_validator(input_value, *args):
        if not input_value or input_value.find(' ') != -1 or input_value == '':
            return None
        else:
            return input_value

    if options.hosts_file:
        new_hosts = read_hosts_file(options.hosts_file)
    else:
        new_hosts = ask_list(None,
                             "\nEnter a comma separated list of new hosts you want\n" \
                             "to add to your array.  Do not include interface hostnames.\n" \
                             "**Enter a blank line to only add segments to existing hosts**", [])
        new_hosts = [host.strip() for host in new_hosts]

    num_new_hosts = len(new_hosts)

    mirror_type = 'none'

    if gparray.get_mirroring_enabled():
        if num_new_hosts < 2:
            raise ExpansionError('You must be adding two or more hosts when expanding a system with mirroring enabled.')
        mirror_type = ask_string(
            "\nYou must now specify a mirroring strategy for the new hosts.  Spread mirroring places\n" \
            "a given hosts mirrored segments each on a separate host.  You must be \n" \
            "adding more hosts than the number of segments per host to use this. \n" \
            "Grouped mirroring places all of a given hosts segments on a single \n" \
            "mirrored host.  You must be adding at least 2 hosts in order to use this.\n\n",
            "What type of mirroring strategy would you like?",
            'grouped', ['spread', 'grouped'])

    try:
        gparray.addExpansionHosts(new_hosts, mirror_type)
        gparray.validateExpansionSegs()
    except Exception, ex:
        num_new_hosts = 0
        if ex.__str__() == 'No new hosts to add':
            print
            print '** No hostnames were given that do not already exist in the **'
            print '** array. Additional segments will be added existing hosts. **'
        else:
            raise

    help = """
    By default, new hosts are configured with the same number of primary
    segments as existing hosts.  Optionally, you can increase the number
    of segments per host.

    For example, if existing hosts have two primary segments, entering a value
    of 2 will initialize two additional segments on existing hosts, and four
    segments on new hosts.  In addition, mirror segments will be added for
    these new primary segments if mirroring is enabled.
    """
    num_new_datadirs = ask_int(help, "How many new primary segments per host do you want to add?", None, 0, 0, 128)

    if num_new_datadirs > 0:
        new_datadirs = []
        new_fsDirs = []
        new_mirrordirs = []
        new_mirrorFsDirs = []

        gpFSobjList = gparray.getFilespaces(includeSystemFilespace=False)

        for i in range(1, num_new_datadirs + 1):
            new_datadir = ask_input(None, 'Enter new primary data directory %d' % i, '',
                                    '/data/gpdb_p%d' % i, datadir_validator, None)
            new_datadirs.append(new_datadir.strip())

            fsDict = {}
            for fsObj in gpFSobjList:
                # Prompt the user for a location for each filespace
                fsLoc = ask_input(None
                                  , 'Enter new file space location for file space name: %s' % fsObj.getName()
                                  , ''
                                  , ''
                                  , datadir_validator
                                  , None
                                  )
                fsDict[fsObj.getOid()] = fsLoc.strip()
            new_fsDirs.append(fsDict)

        if len(new_datadirs) != num_new_datadirs:
            raise ExpansionError(
                'The number of data directories entered does not match the number of primary segments added')

        if gparray.get_mirroring_enabled():
            for i in range(1, num_new_datadirs + 1):
                new_mirrordir = ask_input(None, 'Enter new mirror data directory %d' % i, '',
                                          '/data/gpdb_m%d' % i, datadir_validator, None)
                new_mirrordirs.append(new_mirrordir.strip())

                fsDict = {}
                for fsObj in gpFSobjList:
                    # Prompt the user for a location for each filespace
                    fsLoc = ask_input(None
                                      , 'Enter new file space location for file space name: %s' % fsObj.getName()
                                      , ''
                                      , ''
                                      , datadir_validator
                                      , None
                                      )
                    fsDict[fsObj.getOid()] = fsLoc.strip()
                new_mirrorFsDirs.append(fsDict)

            if len(new_mirrordirs) != num_new_datadirs:
                raise ExpansionError(
                    'The number of new mirror data directories entered does not match the number of segments added')

        gparray.addExpansionDatadirs(datadirs=new_datadirs
                                     , mirrordirs=new_mirrordirs
                                     , mirror_type=mirror_type
                                     , fs_dirs=new_fsDirs
                                     , fs_mirror_dirs=new_mirrorFsDirs
                                     )
        try:
            gparray.validateExpansionSegs()
        except Exception, ex:
            if ex.__str__().find('Port') == 0:
                raise ExpansionError(
                    'Current primary and mirror ports are contiguous.  The input file for gpexpand will need to be created manually.')
    elif num_new_hosts == 0:
        raise ExpansionError('No new hosts or segments were entered.')

    print "\nGenerating configuration file...\n"

    outfile = _gp_expand.generate_inputfile()
    outFilespaceFileName = _gp_expand.generate_filespaces_inputfile(outFileNamePrefix=outfile)

    outFileStr = ""
    if outfile != None:
        outFileStr = """\nInput configuration files were written to '%s' and '%s'.""" % (outfile, outFilespaceFileName)
    else:
        outFileStr = """\nInput configuration file was written to '%s'.""" % (outfile)

    print outFileStr
    print """Please review the file and make sure that it is correct then re-run
with: gpexpand -i %s %s
                """ % (outfile, '-D %s' % options.database if options.database else '')


def sig_handler(sig):
    if _gp_expand is not None:
        _gp_expand.shutdown()

    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)

    # raise sig
    os.kill(os.getpid(), sig)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main(options, args, parser):
    global _gp_expand

    remove_pid = True
    try:
        # setup signal handlers so we can clean up correctly
        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGHUP, sig_handler)

        logger = get_default_logger()
        setup_tool_logging(EXECNAME, getLocalHostname(), getUserName())

        options, args = validate_options(options, args, parser)

        if options.verbose:
            enable_verbose_logging()

        if is_gpexpand_running(options.master_data_directory):
            logger.error('gpexpand is already running.  Only one instance')
            logger.error('of gpexpand is allowed at a time.')
            remove_pid = False
            sys.exit(1)
        else:
            create_pid_file(options.master_data_directory)

        # prepare provider for updateSystemConfig
        gpEnv = GpMasterEnvironment(options.master_data_directory, True)
        configurationInterface.registerConfigurationProvider(
            configurationImplGpdb.GpConfigurationProviderUsingGpdbCatalog())
        configurationInterface.getConfigurationProvider().initializeProvider(gpEnv.getMasterPort())

        dburl = dbconn.DbURL()
        if options.database:
            dburl.pgdb = options.database

        gpexpand_db_status = gpexpand.prepare_gpdb_state(logger, dburl, options)

        # Get array configuration
        try:
            gparray = GpArray.initFromCatalog(dburl, utility=True)
        except DatabaseError, ex:
            logger.error('Failed to connect to database.  Make sure the')
            logger.error('Greenplum instance you wish to expand is running')
            logger.error('and that your environment is correct, then rerun')
            logger.error('gexpand ' + ' '.join(sys.argv[1:]))
            gpexpand.get_gpdb_in_state(GPDB_STARTED, options)
            sys.exit(1)

        _gp_expand = gpexpand(logger, gparray, dburl, options, parallel=options.parallel)

        gpexpand_file_status = None
        if not gpexpand_db_status:
            gpexpand_file_status = _gp_expand.get_state()

        if options.clean and gpexpand_db_status is not None:
            _gp_expand.cleanup_schema(gpexpand_db_status)
            logger.info('Cleanup Finished.  exiting...')
            sys.exit(0)

        if options.rollback:
            try:
                if gpexpand_db_status:
                    logger.error('A previous expansion is either in progress or has')
                    logger.error('completed.  Since the setup portion of the expansion')
                    logger.error('has finished successfully there is nothing to rollback.')
                    sys.exit(1)
                if gpexpand_file_status is None:
                    logger.error('There is no partially completed setup to rollback.')
                    sys.exit(1)
                _gp_expand.rollback(dburl)
                logger.info('Rollback complete.  Greenplum Database can now be started')
                sys.exit(0)
            except ExpansionError, e:
                logger.error(e)
                sys.exit(1)

        if gpexpand_db_status == 'SETUP DONE' or gpexpand_db_status == 'EXPANSION STOPPED':
            if not _gp_expand.validate_max_connections():
                raise ValidationError()
            _gp_expand.perform_expansion()
        elif gpexpand_db_status == 'EXPANSION STARTED':
            logger.info('It appears the last run of gpexpand did not exit cleanly.')
            logger.info('Resuming the expansion process...')
            if not _gp_expand.validate_max_connections():
                raise ValidationError()
            _gp_expand.perform_expansion()
        elif gpexpand_db_status == 'EXPANSION COMPLETE':
            logger.info('Expansion has already completed.')
            logger.info('If you want to expand again, run gpexpand -c to remove')
            logger.info('the gpexpand schema and begin a new expansion')
        elif gpexpand_db_status == None and gpexpand_file_status == None and options.filename:
            if not _gp_expand.validate_unalterable_tables():
                raise ValidationError()
            if _gp_expand.check_unique_indexes():
                logger.info("Tables with unique indexes exist.  Until these tables are successfully")
                logger.info("redistributed, unique constraints may be violated.  For more information")
                logger.info("on this issue, see the Greenplum Database Administrator Guide")
                if not options.silent:
                    if not ask_yesno(None, "Would you like to continue with System Expansion", 'N'):
                        raise ValidationError()
            _gp_expand.validate_heap_checksums()
            newSegList = _gp_expand.read_input_files()
            _gp_expand.addNewSegments(newSegList)
            _gp_expand.sync_packages()
            _gp_expand.start_prepare()
            _gp_expand.add_segments()
            _gp_expand.update_original_segments()
            _gp_expand.update_catalog()
            _gp_expand.move_filespaces()
            _gp_expand.configure_new_segment_filespaces()
            _gp_expand.cleanup_new_segments()
            _gp_expand.setup_schema()
            _gp_expand.prepare_schema()
            logger.info('Starting Greenplum Database')
            GpStart.local('gpexpand expansion prepare final start')
            _gp_expand.sync_new_mirrors()
            logger.info('************************************************')
            logger.info('Initialization of the system expansion complete.')
            logger.info('To begin table expansion onto the new segments')
            logger.info('rerun gpexpand')
            logger.info('************************************************')
        elif options.filename is None and gpexpand_file_status == None:
            interview_setup(gparray, options)
        else:
            logger.error('The last gpexpand setup did not complete successfully.')
            logger.error('Please run gpexpand -r to rollback to the original state.')

        logger.info("Exiting...")
        sys.exit(0)

    except ValidationError:
        logger.info('Bringing Greenplum Database back online...')
        if _gp_expand is not None:
            _gp_expand.shutdown()
        gpexpand.get_gpdb_in_state(GPDB_STARTED, options)
        sys.exit()
    except Exception, e:
        if options and options.verbose:
            logger.exception("gpexpand failed. exiting...")
        else:
            logger.error("gpexpand failed: %s \n\nExiting..." % e)
        if _gp_expand is not None and _gp_expand.pastThePointOfNoReturn == True:
            logger.error(
                'gpexpand is past the point of rollback. Any remaining issues must be addressed outside of gpexpand.')
        if _gp_expand is not None:
            if gpexpand_db_status is None and _gp_expand.get_state() is None:
                logger.info('Bringing Greenplum Database back online...')
                gpexpand.get_gpdb_in_state(GPDB_STARTED, options)
            else:
                if _gp_expand.pastThePointOfNoReturn == False:
                    logger.error('Please run \'gpexpand -r%s\' to rollback to the original state.' % (
                        '' if not options.database else ' -D %s' % options.database))
            _gp_expand.shutdown()
        sys.exit(3)
    except KeyboardInterrupt:
        # Disable SIGINT while we shutdown.
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        if _gp_expand is not None:
            _gp_expand.shutdown()

        # Re-enabled SIGINT
        signal.signal(signal.SIGINT, signal.default_int_handler)

        sys.exit('\nUser Interrupted')


    finally:
        try:
            if remove_pid and options:
                remove_pid_file(options.master_data_directory)
        except NameError:
            pass

        if _gp_expand is not None:
            _gp_expand.halt_work()

if __name__ == '__main__':
    options, args, parser = parseargs()
    main(options, args, parser)