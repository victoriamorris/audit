#!/usr/bin/env python
# -*- coding: utf8 -*-

"""A tool to perform an audit of the FULL catalogue in Catalogue Bridge."""

# Import required modules
# These should all be contained in the standard library
from collections import OrderedDict
import datetime
import gc
import getopt
import itertools
import locale
import os
import re
import sys

# Set locale to assist with sorting
locale.setlocale(locale.LC_ALL, '')

# Set threshold for garbage collection (helps prevent the program run out of memory)
gc.set_threshold(300, 3, 3)

# ====================
#     Constants
# ====================

LEADER_LENGTH, DIRECTORY_ENTRY_LENGTH = 24, 12
SUBFIELD_INDICATOR, END_OF_FIELD, END_OF_RECORD = chr(0x1F), chr(0x1E), chr(0x1D)
ALEPH_CONTROL_FIELDS = ['DB ', 'SYS']

DATE_RANGES = ['Total for all years', 'Pre-Aleph implementation', 'Post-Aleph implementation',
               'No date entered on file', 'Process year: Total', 'Process year: Pre-Aleph implementation',
               'Process year: Post-Aleph implementation', 'Process year: No date entered on file']

EXCLUSIONS = {
    'STA_FFP':      ['STA SUPPRESSED',  '932/STA suppressed and no 949/FFP'],
    '979':          ['979 $j N',        '979 $j N and none of the following: 082, 245 $h electronic resource, '
                                        '538 $a internet, 600-662, 852 $b STI, HMNTS, OC, NPL, MAPS or MUSIC, '
                                        '922/LKR $a ANA, 949/FFP, 952/UNO'],
    '930_SRC_dss':  ['SRC $a DSS02-04', '930/SRC $a DSS02, DSS03 or DSS04 and none of the following: 082, '
                                        '245 $h electronic resource, 260, 264, 300, 538 $a internet, 600-662, 908/CFI, '
                                        '920/LEO, 949/FFP, 952/UNO'],
    '930_SRC_mop':  ['SRC $a MOP',      '930/SRC $a MOP and none of the following: 082, 600-662, 852 $j, 920/LEO'],
    '930_SRC_lds':  ['SRC $a LDS',      '930/SRC $a LDS and none of the following: 082, 600-662, 852, 908/CFI, '
                                        '920/LEO'],
    'other':        ['Other',           'None of the following: LDR/17=5, 082, 600-662, 852, 913/FIN, 922/LKR, '
                                        '928/SID, 949/FFP, 952/UNO, 985 $a LDLSCP or ELECTRONIC'],
}

LEADER_VALIDATION = [[], [], [], [], [],
                     ['a', 'c', 'd', 'n', 'p'],
                     ['a', 'c', 'd', 'e', 'f', 'g', 'i', 'j', 'k', 'm', 'o', 'p', 'r', 't'],
                     ['a', 'b', 'c', 'd', 'i', 'm', 's'],
                     [' ', 'a'],
                     [' ', 'a'],
                     [], [], [], [], [], [], [],
                     [' ', '1', '2', '3', '4', '5', '6', '7', '8', 'u', 'z'],
                     [' ', 'a', 'c', 'i', 'n', 'u'],
                     [' ', 'a', 'b', 'c']]


# ====================
#   Global variables
# ====================
global record_count


# ====================
#     Exceptions
# ====================


class RecordLengthError(Exception):
    def __str__(self): return 'Invalid record length in first 5 bytes of record'


class LeaderError(Exception):
    def __str__(self): return 'Error reading record leader'


class DirectoryError(Exception):
    def __str__(self): return 'Record directory is invalid'


class FieldsError(Exception):
    def __str__(self): return 'Error locating fields in record'


class BaseAddressLengthError(Exception):
    def __str__(self): return 'Base address exceeds size of record'


class BaseAddressError(Exception):
    def __str__(self): return 'Error locating base address of record'


# ====================
#       Classes
# ====================


class MARCReader(object):

    def __init__(self, marc_target):
        super(MARCReader, self).__init__()
        if hasattr(marc_target, 'read') and callable(marc_target.read):
            self.file_handle = marc_target

    def __iter__(self):
        return self

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    def __next__(self):
        first5 = self.file_handle.read(5)
        if not first5: raise StopIteration
        if len(first5) < 5: raise RecordLengthError
        return Record(first5 + self.file_handle.read(int(first5) - 5))


class Record(object):
    global record_count

    def __init__(self, data='', leader=' ' * LEADER_LENGTH):
        self.leader = '{}22{}4500'.format(leader[0:10], leader[12:20])
        self.fields = list()
        self.pos = 0

        self.ID = ''
        self.date_entered = 'No date entered on file'
        self.pub_year = 'None'
        self.language = ''
        self.pub_country = ''
        self.FMT, self.LEO, self.MT = set(), set(), set()

        self.exclude = False
        
        self.q = {
            'pub_year': False, 
            'language': False, 
            'pub_country': False,
            '245h': False,
            '538a': False,
            '852b': False,
            '852j': False,
            '979j': False,
            '985a': False,
            'FFP': False,
            'LKR': False,
            '930_SRC_dss': False,
            '930_SRC_lds': False,
            '930_SRC_mop': False,
            'STA': False,
            'Subjects': False
        }

        if len(data) > 0: self.decode_marc(data)

    def __str__(self):
        text_list = ['=LDR  {}'.format(self.leader)]
        text_list.extend([str(field) for field in self.fields])
        return '\n'.join(text_list) + '\n'

    def __getitem__(self, tag):
        fields = self.get_fields(tag)
        if len(fields) > 0: return fields[0]
        return None

    def __contains__(self, tag):
        fields = self.get_fields(tag)
        return len(fields) > 0

    def __iter__(self):
        self.__pos = 0
        return self

    def __next__(self):
        if self.__pos >= len(self.fields): raise StopIteration
        self.__pos += 1
        return self.fields[self.__pos - 1]

    def add_field(self, *fields):
        self.fields.extend(fields)

    def get_fields(self, *args):
        if len(args) == 0: return self.fields
        return [f for f in self.fields if f.tag in args]

    def decode_marc(self, marc):
        # Extract record leader
        try: self.leader = marc[0:LEADER_LENGTH].decode('ascii')
        except: print('Problem with leader at record: {}'.format(str(record_count)))
        if len(self.leader) != LEADER_LENGTH: raise LeaderError

        # Extract the byte offset where the record data starts
        base_address = int(marc[12:17])
        if base_address <= 0: raise BaseAddressError
        if base_address >= len(marc): raise BaseAddressLengthError

        # Extract directory
        # base_address-1 is used since the directory ends with an END_OF_FIELD byte
        directory = marc[LEADER_LENGTH:base_address - 1].decode('ascii')

        # Determine the number of fields in record
        if len(directory) % DIRECTORY_ENTRY_LENGTH != 0:
            raise DirectoryError
        field_total = len(directory) / DIRECTORY_ENTRY_LENGTH

        # Add fields to record using directory offsets
        field_count = 0
        while field_count < field_total:
            entry_start = field_count * DIRECTORY_ENTRY_LENGTH
            entry_end = entry_start + DIRECTORY_ENTRY_LENGTH
            entry = directory[entry_start:entry_end]
            entry_tag = entry[0:3]
            entry_length = int(entry[3:7])
            entry_offset = int(entry[7:12])
            entry_data = marc[base_address + entry_offset:base_address + entry_offset + entry_length - 1]

            # Check if tag is a control field
            if str(entry_tag) < '010' and entry_tag.isdigit():
                field = Field(tag=entry_tag, data=entry_data.decode('utf-8'))
            elif str(entry_tag) in ALEPH_CONTROL_FIELDS:
                field = Field(tag=entry_tag, data=entry_data.decode('utf-8'))

            else:
                subfields = list()
                subs = entry_data.split(SUBFIELD_INDICATOR.encode('ascii'))
                # Missing indicators are recorded as blank spaces.
                # Extra indicators are ignored.

                subs[0] = subs[0].decode('ascii') + '  '
                first_indicator, second_indicator = subs[0][0], subs[0][1]

                for subfield in subs[1:]:
                    if len(subfield) == 0: continue
                    code, data = subfield[0:1].decode('ascii'), subfield[1:].decode('utf-8', 'strict')
                    subfields.append(code)
                    subfields.append(data)
                field = Field(
                    tag=entry_tag,
                    indicators=[first_indicator, second_indicator],
                    subfields=subfields,
                )
            self.add_field(field)
            field_count += 1

        if field_count == 0: raise FieldsError


class Field(object):

    def __init__(self, tag, indicators=None, subfields=None, data=''):
        if indicators is None: indicators = []
        if subfields is None: subfields = []
        indicators = [str(x) for x in indicators]

        # Normalize tag to three digits
        self.tag = '%03s' % tag

        # Check if tag is a control field
        if self.tag < '010' and self.tag.isdigit():
            self.data = str(data)
        elif self.tag in ALEPH_CONTROL_FIELDS:
            self.data = str(data)
        else:
            self.indicator1, self.indicator2 = self.indicators = indicators
            self.subfields = subfields

    def __iter__(self):
        self.__pos = 0
        return self

    def __str__(self):
        if self.is_control_field():
            text = '=%s  %s' % (self.tag, self.data.replace(' ', '\\'))
        elif self.tag in ALEPH_CONTROL_FIELDS:
            text = '=%s  %s' % (self.tag, self.data.replace(' ', '\\'))
        else:
            text = '=%s  ' % (self.tag)
            for indicator in self.indicators:
                if indicator in (' ', '\\'):
                    text += '\\'
                else:
                    text += '%s' % indicator
            for subfield in self: text += ('$%s%s' % subfield)
        return text

    def __getitem__(self, subfield):
        subfields = self.get_subfields(subfield)
        if len(subfields) > 0:
            return subfields[0]
        return None

    def __contains__(self, subfield):
        subfields = self.get_subfields(subfield)
        return len(subfields) > 0

    def __next__(self):
        if not hasattr(self, 'subfields'):
            raise StopIteration
        while self.__pos < len(self.subfields):
            subfield = (self.subfields[self.__pos],
                        self.subfields[self.__pos + 1])
            self.__pos += 2
            return subfield
        raise StopIteration

    def get_subfields(self, *codes):
        values = []
        for subfield in self:
            if len(codes) == 0 or subfield[0] in codes:
                values.append(str(subfield[1]))
        return values

    def is_control_field(self):
        if self.tag < '010' and self.tag.isdigit(): return True
        if self.tag in ALEPH_CONTROL_FIELDS: return True
        return False


class OutputValues:
    def __init__(self):
        self.values = OrderedDict([
            ('Total for all years', 0),
            ('008 Date', 0),
            ('008 Country', 0),
            ('008 Language', 0),
            ('082', 0),
            ('337 unmediated', 0),
            ('337 computer', 0),
            ('Other 337', 0),
            ('6XX', 0),
            ('920/LEO $a MP1', 0),
            ('920/LEO $a MP15', 0),
            ('920/LEO $a MP17', 0)
        ])


class Stats:
    def __init__(self):
        self.values = {
            'Total for all years': {},
            'Pre-Aleph implementation': {},
            'Post-Aleph implementation': {},
            'No date entered on file': {},
            'Process year: Total': {},
            'Process year: Pre-Aleph implementation': {},
            'Process year: Post-Aleph implementation': {},
            'Process year: No date entered on file': {},
        }
        self.fmt = set()
        self.fmt.add('All formats')
        self.exclusions = {
            'STA_FFP': set(),
            '979': set(),
            '930_SRC_dss': set(),
            '930_SRC_mop': set(),
            '930_SRC_lds': set(),
            'other': set(),
        }

# ====================
#      Functions
# ====================


def exit_prompt(message=''):
    """Function to exit the program after prompting the use to press Enter"""
    if message != '':
        print(str(message))
    input('\nPress [Enter] to exit...')
    sys.exit()


def check_file_location(file_path, function, file_ext='', exists=False):
    """Function to check whether a file exists and has the correct file extension."""
    folder, file, ext = '', '', ''
    if file_path == '':
        exit_prompt('Error: Could not parse path to {} file'.format(function))
    try:
        file, ext = os.path.splitext(os.path.basename(file_path))
        folder = os.path.dirname(file_path)
    except:
        exit_prompt('Error: Could not parse path to {} file'.format(function))
    if file_ext != '' and ext != file_ext:
        exit_prompt('Error: The specified file should have the extension {}'.format(file_ext))
    if exists and not os.path.isfile(os.path.join(folder, file + ext)):
        exit_prompt('Error: The specified {} file cannot be found'.format(function))
    return folder, file, ext

# ====================


def usage():
    """Function to print information about the script"""
    print('Correct syntax is:')
    print('audit [OPTIONS]')
    print('\nOptions:')
    print('    -i       INPUT_FOLDER - Path to folder containing input files.')
    print('    -o       OUTPUT_FOLDER - Path to folder to save output files.')
    print('    --debug  Debug mode.')
    print('    --help   Display this help message and exit.')
    print('\nIf INPUT_FOLDER is not set, files to be audited are assumed to be present in the current folder.')
    print('If OUTPUT_FOLDER is not set, output files are created in the current folder.')
    print('Files to be audited must have named of the form full*.lex, where * is a number.')
    exit_prompt()

# ====================
#      Main code
# ====================


def main(argv=None):
    if argv is None: name = str(sys.argv[1])

    # Global variables
    global record_count

    input_folder, output_folder = '', ''
    debug = False

    print('========================================')
    print('Audit')
    print('========================================')
    print('A tool to perform an audit of the FULL catalogue in Catalogue Bridge\n')

    try:
        opts, args = getopt.getopt(argv, 'i:o:', ['input_folder=', 'output_folder=', 'debug', 'help'])
    except getopt.GetoptError as err:
        exit_prompt('Error: {}'.format(err))
    for opt, arg in opts:
        if opt == '--help':
            usage()
        elif opt == '--debug':
            debug = True
        elif opt in ['-i', '--input_folder']:
            input_folder = arg
        elif opt in ['-o', '--output_folder']:
            output_folder = arg
        else: exit_prompt('Error: Option {} not recognised'.format(opt))

    # Check file locations
    if input_folder != '' and not os.path.exists(output_folder):
        exit_prompt('Error: Could not locate folder for input files')
    if output_folder != '':
        try:
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
        except os.error: exit_prompt('Error: Could not create folder for output files')

    # --------------------
    # Parameters seem OK => start program
    # --------------------

    # Display confirmation information about the transformation
    if input_folder != '':
        print('Input folder: {}'.format(input_folder))
    if output_folder != '':
        print('Output folder: {}'.format(output_folder))
    if debug:
        print('Debug mode')

    stats = Stats()

    try: process_year = str(datetime.datetime.today().year - 1)
    except:
        process_year = '2015'
        print('Cannot determine process year: default is 2015')
    if debug:
        print('Process year: {}'.format(process_year))

    # --------------------
    # Main transformation
    # --------------------

    print('\nStarting transformation ...')
    print('----------------------------------------')
    print(str(datetime.datetime.now()))

    if debug:
        files = [f for f in os.listdir(input_folder if input_folder != '' else '.')
                 if os.path.isfile(f) and re.match(r'^full12\.lex$', str(f))]
    else:
        files = [f for f in os.listdir(input_folder if input_folder != '' else '.')
                 if os.path.isfile(f) and re.match(r'^full[0-9]+\.lex$', str(f))]

    error_file = open(os.path.join(output_folder, 'Errors.txt'), mode='w', encoding='utf-8', errors='replace')

    for f in files:

        print('\n\nProcessing file {0} ...'.format(str(f)))
        print('----------------------------------------')
        print(str(datetime.datetime.now()))
        record_count = 0
        mfile = open(f, 'rb')
        reader = MARCReader(mfile)
        for record in reader:

            # 001
            # ID    # BL record ID
            for field in record.get_fields('001'):
                record.ID = field.data

            if record.ID == '':
                error_file.write('Record without ID at position {} in file {}\n'.format(str(record_count), str(f)))

            if record.ID != '':
                record_count += 1
                if debug and record_count > 1000: break
                print('\r{0} MARC records processed'.format(str(record_count)), end='\r')

                # LEADER (for validation only)
                for i, v in enumerate(LEADER_VALIDATION):
                    if v != [] and len(record.leader) >= i and record.leader[i] not in v:
                        error_file.write('Record {} has invalid LDR position {}: {}\n'.format(
                            record.ID, str(i), record.leader[i]))

                # 008
                # Date entered on file
                # Year of publication
                # Language
                # Place of publication
                for field in record.get_fields('008'):
                    try:
                        date = re.sub(r'[^0-9]', '', field.data[0:6])
                        if int(date[:2]) <= 30: date = '20' + date
                        else: date = '19' + date
                        if int(date) < 20040601: record.date_entered = 'Pre-Aleph implementation'
                        else: record.date_entered = 'Post-Aleph implementation'
                    except:
                        record.date_entered = 'No date entered on file'
                    try:
                        record.pub_year = re.sub(r'[^0-9u]', '', field.data[7:11].lower())
                        if len(record.pub_year) == 4:
                            record.q['pub_year'] = True
                            if 'u' in record.pub_year or record.pub_year in ['0000', '9999']:
                                record.pub_year = 'Other'
                            elif int(record.pub_year) > 2020:
                                error_file.write('Record with strange year of publication: {} ({})'.format(
                                    record.ID, record.pub_year))
                            elif int(record.pub_year) < 1000:
                                error_file.write('Record with strangely early year of publication: {} ({})'.format(
                                    record.ID, record.pub_year))
                        else:
                            record.pub_year = 'None'
                            record.q['pub_year'] = False
                    except:
                        record.pub_year = 'None'
                        record.q['pub_year'] = False

                    try:
                        record.language = re.sub(r'[^a-z]', '', field.data[35:38])
                        record.q['language'] = 2 <= len(record.language) <= 3
                    except:
                        record.q['language'] = False

                    try:
                        record.pub_country = re.sub(r'[^a-z]', '', field.data[15:18].lower())
                        record.q['pub_country'] = 2 <= len(record.pub_country) <= 3
                    except:
                        record.q['pub_country'] = False

                # 245 $h
                record.q['245h'] = any('ELECTRONIC RESOURCE' in subfield.upper()
                                       for subfield in field.get_subfields('h') for field in record.get_fields('245'))

                # 337 $a
                # Media type
                for field in record.get_fields('337'):
                    for subfield in field.get_subfields('a'):
                        record.MT.add(subfield.lower())

                # 538 $a
                record.q['538a'] = any('INTERNET' in subfield.upper() for subfield in field.get_subfields('a')
                                       for field in record.get_fields('538'))

                # 600-662
                # Subjects
                record.q['Subjects'] = any(f in record for f in ['600', '610', '611', '630', '647', '648', '650', '651',
                                                                 '653', '654', '655', '656', '657', '658', '662'])

                # 852
                # Shelfmark
                record.q['852j'] = any('j' in field for field in record.get_fields('852'))
                record.q['852b'] = any( any(s in subfield.upper() for s in ['HMNTS', 'MAPS', 'MUSIC', 'NPL', 'OC', 'STI'])
                                        for subfield in field.get_subfields('b') for field in record.get_fields('852'))

                # 914, FMT
                # Format
                for field in record.get_fields('914', 'FMT'):
                    for subfield in field.get_subfields('a'):
                        record.FMT.add(subfield.upper().strip())
                        stats.fmt.add(subfield.upper().strip())

                # 920, LEO
                # LEO (Library Export Operations) Identifier
                for field in record.get_fields('920', 'LEO'):
                    for subfield in field.get_subfields('a'):
                        subfield = subfield.upper()
                        for s in ['MP1', 'MP15', 'MP17']:
                            if s in subfield: record.LEO.add(s)

                # 922, LKR
                # Link
                record.q['LKR'] = any('ANA' in subfield.upper() for subfield in field.get_subfields('a')
                                      for field in record.get_fields('922', 'LKR'))

                # 930, SRC
                # Source
                record.q['930_SRC_mop'] = any('MOP' in subfield.upper() for subfield in field.get_subfields('a')
                                              for field in record.get_fields('930', 'SRC'))
                record.q['930_SRC_lds'] = any('LDS' in subfield.upper() for subfield in field.get_subfields('a')
                                              for field in record.get_fields('930', 'SRC'))
                record.q['930_SRC_dss'] = any( any(s in subfield.upper() for s in ['DSS02', 'DSS03', 'DSS04'])
                                               for subfield in field.get_subfields('a')
                                               for field in record.get_fields('930', 'SRC'))

                # 932, STA
                # Status
                record.q['STA'] = any('SUPPRESSED' in subfield.upper() for subfield in field.get_subfields('a')
                                      for field in record.get_fields('932', 'STA'))

                # 949, FFP
                # Flag For Publication
                record.q['FFP'] = any('Y' in subfield.upper() for subfield in field.get_subfields('a')
                                      for field in record.get_fields('949', 'FFP'))

                # 985
                record.q['985a'] = any( any(s in subfield.upper() for s in ['LDLSCP', 'ELECTRONIC'])
                                        for subfield in field.get_subfields('a') for field in record.get_fields('985'))

                # 979
                # Negative shelfmark
                record.q['979j'] = any('N' in subfield.upper() for subfield in field.get_subfields('j')
                                       for field in record.get_fields('979'))

                if record.q['STA'] and not record.q['FFP']:
                    record.exclude = True
                    stats.exclusions['STA_FFP'].add(record.ID)

                if record.q['979j'] \
                   and not any(q for q in [record.q['245h'], record.q['538a'], record.q['852b'], record.q['LKR'],
                                           record.q['Subjects'], ]) \
                   and not any(f in record for f in ['082', '949', 'FFP', '952', 'UNO']):
                    record.exclude = True
                    stats.exclusions['979'].add(record.ID)

                if record.q['930_SRC_dss'] \
                   and not any(q for q in [record.q['245h'], record.q['538a'], record.q['Subjects']]) \
                   and not any(f in record for f in ['082', '260', '264', '300', '920', 'LEO', '908', 'CFI', '949',
                                                     'FFP', '952', 'UNO']):
                    record.exclude = True
                    stats.exclusions['930_SRC_dss'].add(record.ID)

                if record.q['930_SRC_mop'] \
                   and not any(q for q in [record.q['852j'], ]) \
                   and not any(f in record for f in ['082', '920', 'LEO']):
                    record.exclude = True
                    stats.exclusions['930_SRC_mop'].add(record.ID)

                if record.q['930_SRC_lds'] \
                   and not record.q['Subjects'] and not any(f in record for f in ['082', '852', '920', 'LEO', '908',
                                                                                  'CFI']):
                    record.exclude = True
                    stats.exclusions['930_SRC_lds'].add(record.ID)

                if record.leader[17] != '5' \
                   and not any(q for q in [record.q['985a'], record.q['Subjects']]) \
                   and not any(f in record for f in ['082', '852', '913', 'FIN', '922', 'LKR', '928', 'SID',
                                                     '949', 'FFP', '952', 'UNO']):
                    record.exclude = True
                    stats.exclusions['other'].add(record.ID)

                record.FMT.add('All formats')
                py = str('Process year: ' + str(record.date_entered))
                if record.pub_year != '' and not record.exclude:
                    if record.pub_year not in stats.values:
                        stats.values[record.pub_year] = {}

                    for fmt in record.FMT:
                        for v in itertools.chain([record.pub_year, record.date_entered, py], DATE_RANGES):
                            if fmt not in stats.values[v]:
                                stats.values[v][fmt] = OutputValues()
                        for v in itertools.chain([record.pub_year, record.date_entered, 'Total for all years'],
                                                 ['Process year: Total', py] if record.pub_year == process_year
                                                 else []):
                            stats.values[v][fmt].values['Total for all years'] += 1
                            stats.values[v][fmt].values['008 Date'] += int(record.q['pub_year'])
                            stats.values[v][fmt].values['008 Language'] += int(record.q['language'])
                            stats.values[v][fmt].values['008 Country'] += int(record.q['pub_country'])
                            stats.values[v][fmt].values['082'] += int('082' in record)
                            stats.values[v][fmt].values['337 unmediated'] += int('unmediated' in record.MT)
                            stats.values[v][fmt].values['337 computer'] += int('unmediated' not in record.MT
                                                                               and 'computer' in record.MT)
                            stats.values[v][fmt].values['Other 337'] += int('unmediated' not in record.MT
                                                                            and 'computer' not in record.MT
                                                                            and len(record.MT) > 0)
                            stats.values[v][fmt].values['6XX'] += int(record.q['Subjects'])
                            stats.values[v][fmt].values['920/LEO $a MP1'] += int('MP1' in record.LEO)
                            stats.values[v][fmt].values['920/LEO $a MP15'] += int('MP15' in record.LEO)
                            stats.values[v][fmt].values['920/LEO $a MP17'] += int('MP17' in record.LEO)
        mfile.close()
    error_file.close()

    # Create union of all exclusion categories
    exclusions = list(set().union(*stats.exclusions.values()))

    for e in EXCLUSIONS:
        ofile = open(os.path.join(output_folder, 'Exclusions - {} - {} records.txt'.format(EXCLUSIONS[e][0], str(len(stats.exclusions[e])))),
                     mode='w', encoding='utf-8', errors='replace')
        for item in sorted(stats.exclusions[e]):
            ofile.write(str(item) + '\n')
        ofile.close()

    now = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    ofile = open(os.path.join(output_folder, 'Catalogue audit summary {}.txt'.format(now)), mode='w', encoding='utf-8', errors='replace')
    ofile.write('Audit of Catalogue Bridge files\n{}\n==============================\n\n'
                'Exclusions\n------------------------------\n'.format(now))
    for e in EXCLUSIONS:
        ofile.write('{}:\t{}\n'.format(str(len(stats.exclusions[e])), EXCLUSIONS[e][1]))
    ofile.write('{0}:\t932/STA suppressed and no 949/FFP\n'.format(str(len(stats.exclusions['STA_FFP']))))
    ofile.write('\nTotal: {0}:\t(note that some records are included in more than one exclusion category)\n'.format(
        str(len(exclusions))))
    ofile.close()
    
    ofile = open(os.path.join(output_folder, 'Catalogue audit data {}.tsv'.format(now)), mode='w', encoding='utf-8', errors='replace')
    ofile.write('YEAR\t' + '\t\t\t\t\t\t\t\t\t\t\t\t'.join(sorted(stats.fmt)) + '\n')
    for i in range(0, len(stats.fmt)):
        ofile.write('\tTotal\t008 Date\t008 Country\t008 Language\t\'082\t337 unmediated\t337 computer\tOther 337'
                    '\t6XX\t920/LEO $a MP1\t920/LEO $a MP15\t920/LEO $a MP17')
    ofile.write('\n')

    for v in DATE_RANGES:
        ofile.write('{}\t'.format(v))
        for fmt in sorted(stats.fmt):
            if fmt in stats.values[v]:
                for w in stats.values[v][fmt].values:
                    if stats.values[v][fmt].values[w] != 0:
                        ofile.write(str(stats.values[v][fmt].values[w]))
                    ofile.write('\t')
        ofile.write('\n')   
        
    for year in sorted(stats.values, reverse=True):
        if year not in DATE_RANGES:
            ofile.write('{}\t'.format(str(year)))
            for fmt in sorted(stats.fmt):
                if fmt in stats.values[year]:
                    for w in stats.values[year][fmt].values:
                        if stats.values[year][fmt].values[w] != 0:
                            ofile.write(str(stats.values[year][fmt].values[w]))
                        ofile.write('\t')
                else:
                    for w in stats.values['Total for all years'][fmt].values:
                        ofile.write('\t')
            ofile.write('\n')             
    ofile.close()            

    print('\n\nTransformation complete')
    print('----------------------------------------')
    print(str(datetime.datetime.now()))
    sys.exit()

if __name__ == '__main__': main(sys.argv[1:])
