import datetime
from builtins import str
import click
from seaflowpy import errors


FILE_COLUMNS = [
    'FILE', 'DATE', 'FILE DURATION', 'LAT', 'LON', 'CONDUCTIVITY',
    'SALINITY', 'OCEAN TEMP', 'PAR', 'BULK RED', 'STREAM PRESSURE',
    'EVENT RATE'
]

def parse_header(line):
    """Create a dict of column indexes by field name"""
    fields = line.rstrip().split('\t')
    fields = [f.replace('.', ' ') for f in fields]
    d = {x:i for i, x in enumerate(fields)}
    return d

def create_file_field(line, header):
    """Create SeaFlow file name form this line"""
    file_field = None
    fields = line.rstrip().split('\t')
    if ('file' in header) and ('day' in header):
        filei = header['file']
        dayi = header['day']
        file_field = fields[dayi] + '/' + fields[filei] + '.evt'
    elif 'FILE' in header:
        filei = header['FILE']
        if fields[filei].startswith('sds_'):
            parts = fields[filei].split('_')
            file_field = parts[1] + '_' + parts[2] + '/' + parts[3] + '.evt'
    if not file_field:
        raise errors.FileError('Could not create file name from line:\n%s' % line)
    return file_field

def create_date_field(line, header):
    """Create ISO8601 date value from this line"""
    date_field = None
    fields = line.rstrip().split('\t')
    if 'time' in header:
        timei = header['time']
        date_field = fields[timei].replace(' ', 'T') + '+00:00'  # assume UTC
    elif 'computerUTC' in header:
        # Given the following definitions:
        # y = year
        # j = day of year
        # h = hour
        # m = minute
        # s = second
        #
        # Then computerUTC is formatted as yyjjjhhmmss
        cutc = fields[header['computerUTC']]
        assert len(cutc) == len('yyjjjhhmmss')
        year = int('20' + cutc[:2])
        dayofyear = int(cutc[2:5])
        hours = int(cutc[5:7])
        minutes = int(cutc[7:9])
        seconds = int(cutc[9:11])

        # dayofyear to month/day
        delta = datetime.timedelta(days=dayofyear-1, hours=hours, minutes=minutes,
                                   seconds=seconds)
        tmpd = datetime.datetime(year, 1, 1) + delta
        # Final UTC ISO8601 SeaFlow compatible timestamp string
        date_field = tmpd.isoformat()  + '+00:00'
    if not date_field:
        raise errors.FileError('could not create date from line:\n%s' % line)
    return date_field

@click.command()
@click.argument('input-sds', type=click.File())
@click.argument('output-sfl', type=click.File(mode='w', atomic=True))
def sds2sfl_cmd(input_sds, output_sfl):
    """Convert SDS file format to SFL."""
    header = parse_header(input_sds.readline())

    output_sfl.write('\t'.join(FILE_COLUMNS) + '\n')

    missing_fields = {}

    file_duration_field = '180'
    for line in input_sds:
        try:
            file_field = create_file_field(line, header)
            date_field = create_date_field(line, header)
        except errors.FileError as e:
            raise click.ClickException(str(e))

        # Make a copy of header index lookup
        d = dict(header)
        # Capture original fields
        fields = line.rstrip().split('\t')
        # Add new or modified fields
        fields.append(file_duration_field)
        d['FILE DURATION'] = len(fields) - 1
        fields.append(file_field)
        d['FILE'] = len(fields) - 1
        fields.append(date_field)
        d['DATE'] = len(fields) - 1


        # Write SFL subset of fields in correct order
        outfields = []
        for col in FILE_COLUMNS:
            try:
                outfields.append(fields[d[col]])
            except KeyError as e:
                # If column is missing, just output NA and note for later
                outfields.append('NA')
                missing_fields[str(e)] = True

        output_sfl.write('\t'.join(outfields) + '\n')

    if missing_fields:
        click.echo('Some fields were missing from input file: %s' % ' '.join(list(missing_fields.keys())))
