from subprocess import Popen, PIPE
import shlex
import os
import inspect
try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote


def is_condor_job():
    return ('_CONDOR_SLOT' in os.environ)
        

class Job(object):
    
    def __init__(self, executable, requirements=None, error=None, output=None,
                 log=None, arguments=(), debug=False):
        executable = shlex.split(executable)
        self.executable = executable[0]
        self.requirements = requirements
        self.error = error
        self.output = output
        self.log = log
        
        if type(arguments) == str:
            self.arguments = tuple(shlex.split(arguments))
        else:
            self.arguments = arguments
        self.arguments = tuple(executable[1:]) + self.arguments
        
        self.buffer = None
        
        self.debug = debug
    
    def begin(self):
        if self.buffer is not None:
            raise Exception("Job already begun")
        
        self.buffer = ''
        
        for field in ('executable', 'requirements', 'error', 'output', 'log'):
            value = getattr(self, field)
            if value:
                self.set(field, value)
    
    def submit(self):
        if self.buffer is None:
            raise Exception("begin() was not called first")
        
        condor_submit = 'condor_submit' if not self.debug else 'cat'
    #    with Popen(condor_submit, stdin=PIPE) as proc:
    #        proc.communicate(self.buffer.encode())
        proc = Popen(condor_submit, stdin=PIPE)
        proc.communicate(self.buffer.encode())
        proc.stdin.close()
        
        self.buffer = None

    def set(self, field, value):
        if self.buffer is None:
            raise Exception("begin() was not called first")

        self.buffer += "{} = {}\n".format(
                str.capitalize(field),
                value
        )
    
    def queue(self, n=1, arguments=()):
        if self.buffer is None:
            raise Exception("begin() was not called first")
        
        if type(arguments) == str:
            arguments = shlex.split(arguments)
        elif type(arguments) != tuple:
            raise Exception("`arguments` must be a string or a tuple")
        arguments = self.arguments + arguments
        
        if arguments:
            self.buffer += "Arguments = {}\n".format(' '.join(map(cmd_quote, map(str, arguments))))
        
        self.buffer += "Queue {}\n".format(n)
    
    def __enter__(self):
        self.begin()
        return self
    
    def __exit__(self, type, value, traceback):
        self.submit()


class SelfJob(Job):
    
    def __init__(self, python='/usr/bin/python', arguments=(), **kwargs):
        (_, filename, _, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
        arguments = (filename,) + arguments
        super(SelfJob, self).__init__(python, arguments=arguments, **kwargs)


if __name__ == "__main__":
    
    with Job('foo.sh --barbar "hello world"', arguments='foo "hell yeah"', debug=True) as job:
        job.queue(arguments=("hi", 1))
        job.set("Output", "/tmp/test.$(Process)")
        job.queue(10, arguments=("oh no", 2))

