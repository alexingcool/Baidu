<?php
/**
 * @file ModelHandler.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/15 12:21:03
 * @brief
 *
 **/
 
require_once dirname(dirname(__FILE__)).'/models/DBEtl.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlVersion.php';
require_once dirname(dirname(__FILE__)).'/models/DBSo.php';
require_once dirname(dirname(__FILE__)).'/models/DBSoUsage.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlJobTracker.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlHdfs.php';
require_once dirname(dirname(__FILE__)).'/models/DBDispatching.php';
require_once dirname(dirname(__FILE__)).'/models/DBDispatchCond.php';
require_once dirname(dirname(__FILE__)).'/models/DBGen.php';
require_once dirname(dirname(__FILE__)).'/models/DBDep.php';
require_once dirname(dirname(__FILE__)).'/models/DBMappingType.php';
require_once dirname(dirname(__FILE__)).'/models/DBFlow.php';
require_once dirname(dirname(__FILE__)).'/models/DBFile.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlConcerner.php';
require_once dirname(dirname(__FILE__)).'/models/DBDatanode.php';
require_once dirname(dirname(__FILE__)).'/models/DBDataQualitSet.php';
require_once dirname(dirname(__FILE__)).'/models/DBEtlNamespace.php';
require_once dirname(dirname(__FILE__)).'/models/DBFieldMapping.php';
require_once dirname(dirname(__FILE__)).'/components/PackageTool.php';
require_once dirname(dirname(__FILE__)).'/components/ETLTools.php';
require_once dirname(dirname(__FILE__)).'/excutor/ConstDef.php';
 
 class ModelHandler {
 	public function __construct($eid, $evid) {
 		$this->etl = DBEtl::getById($eid);
 		$this->jtid = $this->etl->jtid();
 		$this->fsid = $this->etl->fsid();
 		$this->etlversion = DBEtlVersion::getByEvid($evid);
 		$this->etlconcerners = DBEtlConcerner::getByEid($eid);
 		$this->etlhdfs = DBEtlHdfs::getById($this->fsid);
 		$this->etljobtracker = DBEtlJobTracker::getById($this->jtid);
 		$this->flows = DBFlow::getByEvid($evid);
 		$this->datanodes = DBDatanode::getByEvid($evid);
 	}
 	
 	//get all obj function
 	public function getEtlObj() {
 		if ($this->etl === null || $this->etl === false) {
 			throw new Exception("Etl is illegal");
 		}
 		return $this->etl;
 	}
 	public function getEtlVersionObj() {
 		if ($this->etlversion === null || $this->etlversion === false) {
 			throw new Exception("Etl Version is illegal");
 		}
 		return $this->etlversion;
 	}
 	public function getEtlConcernersObj() {
 		if ($this->etlconcerners === null || $this->etlconcerners === false) {
 			throw new Exception("Etl Concerners are illegal");
 		}
 		return $this->etlconcerners;
 	}
 	public function getEtlHdfsObj() {
 		if ($this->etlhdfs === null || $this->etlhdfs === false) {
 			throw new Exception("Etl Hdfs is illegal");
 		}
 		return $this->etlhdfs;
 	}
 	public function getEtlJobTrackerObj() {
 		if ($this->etljobtracker === null || $this->etljobtracker === false) {
 			throw new Exception("Etl Job Tracker is illegal");
 		}
 		return $this->etljobtracker;
 	}
 	public function getFlowsObj() {
 		$flows = $this->flows;
 		if ($flows === false || $flows === null) {
			throw new Exception("Bad Get Etl Flows, Can not get flow");
		}
		return $flows;
 	}
 	public function getDataNodesObj() {
 		if ($this->datanodes === null || $this->datanodes === false) {
 			throw new Exception("datanodes are illegal");
 		}
 		return $this->datanodes;
 	}
 	
 	//all db static functions
 	public function getAllJobTrackers() {
 		$jobtrackers = DBEtlJobTracker::getAll();
 		if ($jobtrackers === null || $jobtrackers === false) {
 			throw new Exception("jobtrackers are illegal");
 		}
 		return $jobtrackers;
 	}
 	public function getAllHdfses() {
 		$hdfses = DBEtlHdfs::getAll();
 		if ($hdfses === null || $hdfses === false) {
 			throw new Exception("hdfses are illegal");
 		}
 		return $hdfses;
 	}
 	public function getDepByFid($fid) {
 		$deps = DBDep::getByFid($fid);
 		if ($deps === null || $deps === false) {
 			throw new Exception("Bad Get Flow Deps By Fid");
 		}
 		return $deps;
 	}
 	public function getDepByDid($did) {
 		$dep = DBDep::getByDid($did);
 		if ($dep === null || $dep === false) {
 			throw new Exception("Bad Get Flow Deps By Did");
 		}
 		return $dep;
 	}
 	public function getGenByFid($fid) {
 		$gens = DBGen::getByFid($fid);
 		if ($gens === null || $gens === false) {
 			throw new Exception("Bad Get Flow Gens by Fid");
 		}
 		return $gens;
 	}
 	public function getDataNodeType($type) {
 		return DBDatanode::typeIntToStr($type);
 	}
 	public function getDepType($deptype) {
 		$depTypeToStr = DBDatanode::getDepTypes();
 		return $depTypeToStr[$deptype];
 	}
 	public function getGenType($gentype) {
 		$genTypeToStr = DBDatanode::getGenTypes();
 		return $genTypeToStr[$gentype];
 	}
 	
 	private $etl;
 	private $etlversion;
 	private $etlconcerners;
 	private $etlhdfs;
 	private $etljobtracker;
 	private $flows;
 	private $datanodes;
 	private $dep;
 	private $gen;
 	private $etlexcution;
 	private $etlprimary;
 }
