<?php
/**
 * @file DagManager.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/10 16:48:15
 * @brief
 *
 **/
 
 require_once dirname(__FILE__)."/DagGeneration.php";
 require_once dirname(__FILE__)."/Job.php";
 require_once dirname(dirname(__FILE__)) . "/components/DagExecComponents.php";
 
 class DagManager {
 	public function __construct($eid) {
 		$this->eid = $eid;
 	}

	public function getPrimaryVersion() {
        /*
		$instance = ETLTools::getInstance();
		$evid = $instance->getEtlPrimaryVersion($this->eid);
		if ($evid === null) {
			throw new Exception("Failed to Get Etl Primary");
        }
         */
        $evid = 5;
		$this->evid = $evid;
	}
	
	public function run() {
		self::getPrimaryVersion();
		$this->dagGen = new DagGeneration($this->eid, $this->evid);
		$this->daginput = $this->dagGen->genDag();
        $this->job = new Job($this->daginput);
        $this->job->excute();
        $queue = $this->job->getQueue();
        //print "qqqqqqqqqqqqqqqquuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu\n";
        //print_r($queue);
        $this->dagexec = new DagExecComponent($this->evid);
        $this->dagexec->constructExecInfo($this->job->getQueue(), $this->daginput);
	} 	
	
	private $dagGen;
 	private $job;
 	private $daginput;
 	private $eid;
 	private $evid;
 	private $dagexec;
 };

