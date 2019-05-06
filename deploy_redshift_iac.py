import sys,getopt
from redshift_cluster_generator import RedshiftClusterGenerator

def main(argv):
    config_file = '' 
    useExistingRole = False 
    useExistingVpc = False
    
    try:
        program_name = argv[0]
        opts, _ = getopt.getopt(argv[1:],"c:rv")
        for k, v in opts:
            if k =='-c':
                config_file = v
            if k == '-r':
                useExistingRole = True
            if k == '-c':
                useExistingVpc =True
    except getopt.GetoptError:
        print (('{} {} {} {}').format(program_name,'-c <config file>','[-r]','[-v]'))
        print ("use the -r flag when you want to use an existing Iam Role in AWS to access S3")
        print ("use the -v flag when you want to use an already existing Vpc connection")
    
    cluster_gen = RedshiftClusterGenerator(config_file,useExistingRole,useExistingVpc)
    if(cluster_gen.configFileOK == True):
        cluster_gen.generateRedshiftCluster()
    

if __name__ == "__main__":
    main(sys.argv)