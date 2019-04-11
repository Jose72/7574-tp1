from email.utils import formatdate


NEW_LINE = '\n'


class HttpParser:

    @staticmethod
    def check_correct_service(request, petition, url):
        f_line = request.split('\n',1)[0]
        p = f_line.split(' ')
        p_url = p[1].split('?')[0]
        return (p[0] == petition) & (p_url == url)

    @staticmethod
    def parse_get(request):
        queries = ''
        f_line = request.split('\n')  # split by new line
        f_line = [x for x in f_line if x]  # remove empty strings
        queries = f_line[-1]  # get the payload
        return queries

    @staticmethod
    def parse_post(request):
        logs = ''
        f_line = request.split('\n')  # split by new line
        f_line = [x for x in f_line if x]  # remove empty strings
        logs = f_line[-1]  # get the payload
        return logs

    @staticmethod
    def parse(code, request):
        if code == 'GET':
            return HttpParser.parse_get(request)
        if code == 'POST':
            return HttpParser.parse_post(request)
        return ''

    @staticmethod
    def generate_response(status_code, payload):
        r = 'HTTP/1.1 ' + status_code + NEW_LINE + \
            'Date: ' + formatdate(timeval=None, localtime=False, usegmt=True) + NEW_LINE + \
            'Server: ' + 'Server' + NEW_LINE + \
            'Content - Length: ' + str(len(payload)) + NEW_LINE + \
            'Content - Type: ' + 'application/json' + NEW_LINE + \
            'Connection: Closed ' + NEW_LINE + \
            payload + NEW_LINE
        return r


def numb_to_str_with_zeros(num, digits):
    n = str(num)
    z = 0
    if len(n) < digits:
        z = digits - len(n)
    return n.zfill(z)
