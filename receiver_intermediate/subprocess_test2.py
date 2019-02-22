import subprocess

def execute(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = ''

    # Poll process for new output until finished
    for line in iter(process.stdout.readline, ""):
        print line,
        output += line


    process.wait()
    exitCode = process.returncode

    if (exitCode == 0):
        return output
    else:
        raise Exception(command, exitCode, output)

execute(['ping', 'localhost'])