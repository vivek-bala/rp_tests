#!/usr/bin/env python

__author__       = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2016, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Pipeline of Ensembles Example (generic)"

import sys
import os
import json

from radical.entk import AppManager, Kernel, ResourceHandle, PoE, EoP

from kernel_defs.ccount import ccount_kernel
from kernel_defs.chksum import chksum_kernel
import traceback
# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

N = 100000

# ------------------------------------------------------------------------------
#
class Test(EoP):
    """The CalculateChecksums class implements a Bag of Tasks. 
    """

    def __init__(self, ensemble_size, pipeline_size):
        super(Test,self).__init__(ensemble_size, pipeline_size)

    def stage_1(self, instance):

        """
        This stage calculates the number of characters in a UTF file.
        """
        global N

        k = Kernel(name="ccount")
        k.arguments = ["--inputfile=UTF-8-demo.txt", "--outputfile=ccount-{0}.txt".format(instance)]
        k.copy_input_data  = ["$SHARED/UTF-8-demo.txt"]

        for i in range(N):
            k.copy_input_data.append('$SHARED/UTF-8-demo.txt > UTF-8-{0}.txt'.format(i))

        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":


    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: 
        print 'Usage:\t%s [resource]\n\n' % sys.argv[0]
        sys.exit(1)
    elif len(sys.argv) == 2: 
        resource = sys.argv[1]
    else: 
        resource = 'local.localhost'

    try:

        with open('%s/config.json'%os.path.dirname(os.path.abspath(__file__))) as data_file:    
            config = json.load(data_file)

        os.system('wget -q -o UTF-8-demo.txt http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-demo.txt')

        # Create an application manager
        app = AppManager(name='example_1')

        # Register kernels to be used
        app.register_kernels(ccount_kernel)

        # Create a new resource handle with one resource and a fixed
        # number of cores and runtime.
        cluster = ResourceHandle(
                resource=resource,
                cores=config[resource]["cores"],
                walltime=30,
                #username=None,

                project=config[resource]['project'],
                access_schema = config[resource]['schema'],
                queue = config[resource]['queue'],
                database_url='mongodb://rp:rp@ds151993.mlab.com:51993/test_08_22',
            )


        cluster.shared_data = ['./UTF-8-demo.txt']

        # Allocate the resources.
        cluster.allocate(wait=True)

        # Create pattern object with desired ensemble size, pipeline size
        pipe = Test(ensemble_size=16, pipeline_size=1)

        # Add workload to the application manager
        app.add_workload(pipe)

        # Run the given workload
        cluster.run(app)

    except Exception, ex:
        print 'Application failed, error: ', ex
        print traceback.format_exc()
    finally:
        # Deallocate the resource
        cluster.deallocate()
