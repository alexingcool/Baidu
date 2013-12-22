<?php
/**
 * @file DagInfoComponents.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/10 15:12:35
 * @brief
 *
 **/
 
require_once dirname(dirname(__FILE__)).'/components/ModelHandler.php';

class DagInfoComponent {
	public function __construct($eid, $evid) {
		$this->modelhandler = new ModelHandler($eid, $evid);  
        $this->evid = $evid;
	}
	
	private function getJtName($jtid, &$jtname, &$fsname, &$displayer, &$queue, &$ugi) {
		$jobtrackers = $this->modelhandler->getAllJobTrackers();
		$isFind = false;
		foreach ($jobtrackers as $jobtracker) {
			if ($jtid === $jobtracker->jtid()) {
				$jtname = $jobtracker->tracker();
				$fsname = $jobtracker->fs();
				$displayer = $jobtracker->displayer();
				$queue = $jobtracker->queue();
				$ugi = $jobtracker->ugi();
				$isFind = true;
				break;
			}
		}
		if ($isFind === false) {
			throw new Exception("Bad Jtid, Can not Find in etl_jobtracker");
		}
	}
	
	private function getFsName($fsid) {
		$hdfses = $this->modelhandler->getAllHdfses();
		$fsname = null;
		$isFind = false;
		foreach ($hdfses as $hdfs) {
			if ($fsid === $hdfs->fsid()) {
				$fsname = $hdfs->namenode();
				$isFind = true;
				break;
			}
		}
		if ($isFind === false) {
			throw new Exception("Bad Fsid, Can not Find in etl_hdfs");
		}
		return $fsname;
	}
	
	private function getDagAttrVersion() {
		$etlVersion = $this->modelhandler->getEtlVersionObj();
        $this->dagattr[ConstDef::VERSIONNAME] = $etlVersion->name();
		$this->dagattr[ConstDef::ARGS] = $etlVersion->args();
		$startTime = time();
		$etlVersion->setStartTime($startTime);
		if ($etlVersion->update() === false) {
			throw new Exception("Bad Etl Version Update, Can not update etl_version");
		} 
	}
	
	public function getDagAttr() {
		$etl = $this->modelhandler->getEtlObj();
        $etlversion = $this->modelhandler->getEtlVersionObj();
		if ($etl === null) {
			throw new Exception("Get DataNodes By evid Failed");
		}
		$this->dagattr[ConstDef::NAME] = $etl->name();
		$this->dagattr[ConstDef::FREQ] = $etl->interval();
		$this->dagattr[ConstDef::STARTPARTITION] = $etl->start_time(); 		
		$this->dagattr[ConstDef::ENDPARTITION] = $etl->end_time();
		$jtid = $etl->jtid();
		$fsid = $etl->fsid();
		$jtname = null;
		$jtfsname = null;
		$fsname = null;
		$displayer = null;
		$queue = null;
		$ugi = null;
		self::getJtName($jtid, $jtname, $jtfsname, $displayer, $queue, $ugi);
		self::getFsName($fsid, $fsname);
		$deptype = DBEtl::getDepType($etl->deptype());
		$gentype = DBEtl::getGenType($etl->gentype());
		$this->dagattr[ConstDef::NAMENODE] = $jtfsname;
		$this->dagattr[ConstDef::JOBTRACKER] = $jtname;
		$this->dagattr[ConstDef::LOGNAMENODE] = $fsname;
		$this->dagattr[ConstDef::DISPLAYER] = $displayer;
		$this->dagattr[ConstDef::QUEUE] = $queue;
		$this->dagattr[ConstDef::UGI] = $ugi;
		$this->dagattr[ConstDef::DEPTYPE] = $deptype;
		$this->dagattr[ConstDef::GENTYPE] = $gentype;
        $this->dagattr[ConstDef::ENDTIME] = $etlversion->endTime();
        $this->dagattr[ConstDef::DEADLINE] = $etlversion->deadLine();
		
		$etlConcerners = $this->modelhandler->getEtlConcernersObj();
		foreach ($etlConcerners as $concerner) {
			$this->dagattr[ConstDef::CONCERNERS][] = $concerner->username();
		}	
		
		self::getDagAttrVersion();
        return $this->dagattr;
	}
	
	public function getStartNodes() {
		$startnodes = array();
		$datanodes = $this->modelhandler->getDataNodesObj();
        $flows = $this->modelhandler->getFlowsObj();
		$startdids = array();
        $enddids = array();
		foreach ($flows as $flow) {
			$fid = $flow->fid();
			$deps = $this->modelhandler->getDepByFid($fid);
			foreach ($deps as $dep) {
				$did = $dep->did();
				$startdids[$did] = 1;
            } 
            $gens = $this->modelhandler->getGenByFid($fid);
            foreach ($gens as $gen) {
                $did = $gen->did();
                $enddids[$did] = 1;
            }
		}
		foreach ($datanodes as $datanode) {
            $did = $datanode->did();
			if (array_key_exists($did, $startdids) && (!array_key_exists($did, $enddids))) {
				$startnodes[] = $datanode->name();		
			}
		}
		return $startnodes;	
	}
	
	public function getEndNodes() {
		$endnodes = array();
		$datanodes = $this->modelhandler->getDataNodesObj();
        $flows = $this->modelhandler->getFlowsObj();
		$enddids = array();
		foreach ($flows as $flow) {
			$fid = $flow->fid();
			$gens = $this->modelhandler->getGenByFid($fid);
			foreach ($gens as $gen) {
				$did = $gen->did();
				$enddids[$did] = 1;
			} 
		}
		foreach ($datanodes as $datanode) {
			$did = $datanode->did();
			if (array_key_exists($did, $enddids)) {
				$type = $this->modelhandler->getDataNodeType($datanode->type());
				if ($type !== "temp") {
					$endnodes[] = $datanode->name();		
				}
			}
		}
		return $endnodes;	
	}
	
	public function getDataNodes() {
		$rets = array();
		$datanodes = $this->modelhandler->getDataNodesObj();
		$this->datanodes = $datanodes;
		foreach ($datanodes as $datanode) {
            $datanodeAttr = array();
			$path = $datanode->path();
            if ($path !== null && $path !== "") {
		    	$patharr = explode('.', $path);
		    	if (count($patharr) !== 2) {
		    		throw new Exception("Bad Datanode Path");
                }
			}
			$did = $datanode->did();
            if ($patharr[0] !== ConstDef::DEF) {
			    $product = $patharr[0];
            } else {
                $product = null;
            }
			$datanodeAttr[ConstDef::NAME] = $datanode->name();
            $datanodeAttr[ConstDef::DID] = $datanode->did();
			$datanodeAttr[ConstDef::PRODUCT] = $product;
			$datanodeAttr[ConstDef::DEPTIME] = $datanode->depPattern();
			$datanodeAttr[ConstDef::PARTITION] = $datanode->selfPartPattern();
			$datanodeAttr[ConstDef::TYPE] = DBDatanode::typeIntToStr($datanode->type());
			$fsid = $datanode->fsid();
            $fsname = null;
            if ($fsid !== null) {
		    	$fsname = self::getFsName($fsid);
            }
			$datanodeAttr[ConstDef::DATANODEFSNAME] = $fsname;
			$datanodeAttr[ConstDef::DEPTYPE] = $this->modelhandler->getDepType($datanode->deptype());
			$datanodeAttr[ConstDef::GENTYPE] = $this->modelhandler->getGenType($datanode->gentype());
            self::genDepInfo();
            if (array_key_exists($did, $this->depdid2dep)) {
			    $dep = $this->depdid2dep[$did];
			    $datanodeAttr[ConstDef::NODEDISTINCT] = $dep->type();
            } else {
                $datanodeAttr[ConstDef::NODEDISTINCT] = null;
            }
			$did2name[$did] = $datanode->name();
            $rets[] = $datanodeAttr;
		}
		return $rets;
	}

    private function genDepInfo() {
        $flows = $this->modelhandler->getFlowsObj();
        foreach ($flows as $flow) {
            $fid = $flow->fid();
            $flowdeps = $this->modelhandler->getDepByFid($fid);
            foreach ($flowdeps as $flowdep) {
                $depdid = $flowdep->did();
                $this->depdid2dep[$depdid] = $flowdep;
            }
        }
    }
	
	public function getFlows() {
		$flows = $this->modelhandler->getFlowsObj();
        $this->flows = $flows;
		$flowarr = array();
		$index = 0;
		foreach ($flows as $flow) {
			$fid = $flow->fid();
			$flowdeps = $this->modelhandler->getDepByFid($fid);
			$depnodes = array();
            $depdids = array();
			foreach ($flowdeps as $flowdep) {
				$depdid = $flowdep->did();
				$depnodes[] = $this->did2name[$depdid];
                $depdids[] = $depdid;
			}
			$flowarr[$index][ConstDef::FID] = $flow->fid();
			$flowarr[$index][ConstDef::STARTNODES] = $depnodes;
            $flowarr[$index][ConstDef::STARTDID] = $depdids;
			$gennodes = array();
            $gendids = array();
			$flowgens = DBGen::getByFid($fid);
			foreach ($flowgens as $flowgen) {
				$gendid = $flowgen->did();
				$gennodes[] = $this->did2name[$gendid];
                $gendids[] = $gendid;
			}
			$flowarr[$index][ConstDef::ENDNODES] = $gennodes;
            $flowarr[$index][ConstDef::ENDDID] = $gendids;
			$flowarr[$index][ConstDef::TYPE] = DBFlow::typeIntToStr($flow->type());
			$soid = $flow->soid();
			$fileid = $flow->fileid();
			$sfileid = $flow->sfileid();
			if ($soid !== null) {
				$so = DBSo::getBySoid($soid);
				$cluster = 'hdfs://szwg-stoff-hdfs.dmop.baidu.com:54310';
				$packret = PackageTool::getFullPath($cluster, $so->path());
				$sopath = $packret[ConstDef::FULLPATH];
				$flowarr[$index][ConstDef::SO] = $sopath . ".tar";
			}
			if ($fileid !== null) {
				$file = DBFile::getById($fileid);
				$cluster = 'hdfs://szwg-stoff-hdfs.dmop.baidu.com:54310';
				$packret = PackageTool::getFullPath($cluster, $file->path());
				$filepath = $packret[ConstDef::FULLPATH];
				$flowarr[$index][ConstDef::OTHERFILE] = $filepath;
			}
			if ($sfileid !== null) {
				$file = DBFile::getById($sfileid);
				$cluster = 'hdfs://szwg-stoff-hdfs.dmop.baidu.com:54310';
				$packret = PackageTool::getFullPath($cluster, $file->path());
				$filepath = $packret[ConstDef::FULLPATH];
				$flowarr[$index][ConstDef::SHELL] = $filepath;
			}
			$flowarr[$index++][ConstDef::ARGS] = $flow->args();
		}
		return $flowarr;
	}
	
	private $modelhandler;
	private $eid;
	private $evid;
	private $etl;
	private $etlversion;
	private $datanode;
	private $flow;
	private $file;
	private $file_usage;
	private $so;
	private $so_usage;
	private $flows;
	private $datanodes;
	private $did2name;
    private $depdid2dep;
	private $dagattr;
	private $hdfses;
	private $jobtrackers;
};
 
