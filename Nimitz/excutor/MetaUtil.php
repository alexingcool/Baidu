<?php
/**
 * @file MetaUtil.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/12 15:49:27
 * @brief
 *
 **/
 
 require_once dirname(dirname(__FILE__)).'/models/DTMetaOldClient.php';
 require_once dirname(__FILE__) . '/ConstDef.php';

 class MetaUtil {
 	static public function getInstance() {
 		if (self::$instance === null) {
 			self::$instance = new MetaUtil();
 		}
 		return self::$instance;
 	}
 	
 	public function getTableName($node, &$dbname, &$tablename, &$namenode) {
        $type = null;
        $product = $node->product;
        switch ($node->type) {
        case ConstDef::BIGEVENT:
        case ConstDef::EVENT:
            $type = ConstDef::EVENT;
            $product = ConstDef::UDW;
            break;
        case ConstDef::MV:
            $type = ConstDef::MATERIALIZEVIEW;
            break;
        }
        $orgid = DTMetaOldClient::getOrgProduct($product);
        $ret = DTMetaOldClient::getDataByTypeOrgName($type, $orgid, $node->name);
        $dbname = $ret[DTMetaOldClient::DBNAME];
        $tablename = $ret[DTMetaOldClient::TABLENAME];
        $namenode = $ret[DTMetaOldClient::NAMENODE];
        return $tablename;
 	}
 	
 	public function getLogPathFreq($node, &$freq, &$path, &$clustername, &$namenode) {
        $orgid = DTMetaOldClient::getOrgProduct($node->product);
        $ret = DTMetaOldClient::getLogFrequencyAndPath(ConstDef::LOG, $orgid, $node->name);
        $freq = $ret[DTMetaOldClient::FREQUENCY];
        $path = $ret[DTMetaOldClient::PATH];
        $clustername = $ret[DTMetaOldClient::CLUSTERNAME];
        $namenode = $ret[DTMetaOldClient::NAMENODE];
 	}

 	static private $instance;
 }
