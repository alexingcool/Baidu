<?php
/**
 ** @file DagImporterManager.php
 ** @author zhangliang(com@baidu.com)
 ** date 2013/11/26 13:32:05
 ** @brief
 **
 ***/
require_once dirname(__FILE__).'/DagImporter.php';
require_once dirname(__FILE__).'/Exception.php';
require_once dirname(__FILE__).'/DBConnect.php';

class DagImporterManager {
    public function __construct($dir, $file, $nid) {
        $this->_dir = $dir;
        $this->_file = $file;
        $this->_workpath = $dir."/".(string)time();
        $this->_nid = $nid;
        $tarcmd = "rm -rf $this->_workpath; mkdir $this->_workpath; tar -xvf ".$this->_file." -C ".$this->_workpath." >/dev/null 2>&1";
        system($tarcmd);
        DBConnect::getPdo()->beginTransaction();
    }

    public function __destruct() {
        $cmd = "cd $this->_dir; rm -rf $this->_workpath;";
        system($cmd);
    }

    public function Excute() {
        try {
            self::_Prepare();
            self::_ImportXml();
            self::_XmlToSql();
            DBConnect::getPdo()->commit();
        } catch (ImporterException $imerr) {
            $imerr->LogError();
            UB_LOG_WARNING("excute failed info: %s", $imerr->getTraceAsString());
            DBConnect::getPdo()->rollBack();
        } catch (Exception $ex) {
            UB_LOG_WARNING("excute failed info: %s", $ex->getTraceAsString());
            DBConnect::getPdo()->rollBack();
        }
    }

    private function _Prepare() {
        self::_GetAllPath();
        self::_GetMapCount();
        self::_Register();
    }

    private function _GetAllPath() {
        $fp = opendir($this->_workpath);
        $currentpath = null;
        while ($file = readdir($fp)) {
            if ($file === false) {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Read Directory Failed");
            }   
            if ($file != '.' && $file != '..') {
                $currentpath = $this->_workpath."/".$file;
                $this->_dagpath = $currentpath;
                closedir($fp);
                $fp = opendir($currentpath);
                break;
            } else if ($file == '.' || $file == '..') {
                continue;
            } else {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Only Importer One Job");
            }
        }
        while ($file = readdir($fp)) {
            if ($file === false) {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Read Sub Directory Failed");
            }
            if ($file === '.' || $file === '..') {
                continue;
            } 
            $dirpath = $currentpath."/".$file;
            if ($file == 'so') {
                $this->_sodir = $dirpath;
            } else if ($file == 'flow') {
                $this->_flowdir = $dirpath;
            } else if ($file == 'DQConf') {
                $this->_dqdir = $dirpath;
            } else if ($file == 'file') {
                $this->_filedir = $dirpath;
            } else if ($file == 'dag.xml') {
                $this->_dagxml = $file;
            } else {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "Tar Format Not Right");
            }
        }
        closedir($fp);
    }

    private function _GetMapCount() {
        $this->_mapcount = 0;
        $this->_dispcount = 0;
        $this->_maps = array();
        $this->_disps = array();
        $fp = opendir($this->_flowdir);

        while ($file = readdir($fp)) {
            if ($file === false) {
                throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "GetMapXmlPath readdir failed");
            }   
            if ($file === '.' || $file === '..') {
                continue;
            }   
            if (self::_IsMapOrDisp($file) === false) {
                $this->_mapcount++;
                $this->_maps[] = $file;
            } else {
                $this->_dispcount++;
                $this->_disps[] = $file;
            }
        }   
        closedir($fp);
    }

    private function _IsMapOrDisp($file) {
        $xml = simplexml_load_file($this->_flowdir."/".$file);
        $disp = null;
        foreach ($xml->disp as $disp) {
            if ($disp != null) {
                return true;
            }
        }
        foreach ($xml->fieldmap as $map) {
             if ($map != null) {
                 return false;
             }
         }
         foreach ($xml->fieldreduce as $map) {
             if ($map != null) {
                 return false;
             }
         }
         throw new ImporterException(__FILE__, __FUNCTION__, __LINE__, "UnSupported Flow");
    }

    private function _Register() {
        $dagpaths['dag'] = $this->_dagpath;
        $dagpaths['so'] = $this->_sodir;
        $dagpaths['shell'] = $this->_filedir;
        $dagpaths['file'] = $this->_filedir;
        $dagpaths['dq'] = $this->_dqdir;
        $this->_importer[] = new DagImporter($this->_dagxml, $dagpaths, $this->_nid);
        foreach ($this->_maps as $map) {
            $this->_importer[] = new MappingXmlImporter($map, $this->_flowdir, $this->_importer[0]);
        }
        for ($count = 0; $count < $this->_dispcount; $count++) {
            $this->_importer[] = new DispatchXmlImporter($this->_disps[$count], $this->_flowdir, $this->_importer[0]);
        }
    }

    private function _ImportXml() {
        foreach ($this->_importer as $importer) {
            $importer->ImportXml();
        }
    }

    private function _XmlToSql() {
        foreach ($this->_importer as $importer) {
            $importer->XmlToSql();
        }
    }

    private $_workpath;
    private $_dagpath;
    private $_importer;
    private $_dir;
    private $_file;
    private $_dagxml;
    private $_dqdir;
    private $_filedir;
    private $_flowdir;
    private $_sodir;

    private $_maps;
    private $_mapcount;
    private $_disps;
    private $_dispcount;

    private $_nid;
};



