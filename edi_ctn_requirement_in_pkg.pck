create or replace package edi_ctn_requirement_in_pkg is
  /************************************
  *
  * ��������  
  * ���� ����� 2007-6-7
  * ���� @pi_batch_no �������κ�
  *      @pi_user_id ������id
  * �޸���ʷ
  * 
  *************************************/

  procedure import(pi_batch_no in varchar2, -- ���κ�
                   pi_user_id  in varchar2 -- ������Id
                   );

  /************************************
  *
  * �������Զ�����  
  * ���� ����� 2007-11-21
  * ���� @pi_batch_no �������κ�
  *      @po_result ������
  *
  *************************************/

  procedure auto_import(pi_batch_no varchar2, -- ���κ�
                        po_result   out varchar2); -- ������

end edi_ctn_requirement_in_pkg;
/
create or replace package body edi_ctn_requirement_in_pkg is
  /************************************
  *
  *��������  create by snow 2007-6-7
  *
  *************************************/
  
  --��ȡ��������
  procedure getSaparamsConfig(pi_code   in varchar2,
                              pi_org_id in varchar2,
                              pi_paramsValue out saparamsdetail%rowtype)is
  begin
    select sd.sapd_value1,sd.sapd_value2,sd.sapd_value3,sd.sapd_value4,sd.sapd_value5,sd.sapd_value6
      into pi_paramsValue.sapd_value1,pi_paramsValue.sapd_value2,pi_paramsValue.sapd_value3,
           pi_paramsValue.sapd_value4,pi_paramsValue.sapd_value5,pi_paramsValue.sapd_value6
      from saparamsconfig sc , saparamsdetail sd
     where sc.sapc_params_config_id = sd.sapd_params_config_id
       and sc.sapc_params_code = pi_code
       and sd.sapd_org_id = pi_org_id
       and rownum = 1;
  exception
    when others then
      null;
  end getSaparamsConfig;

  function get_plan_id(pi_vessel_name in varchar2,
                       pi_voyage      in varchar2,
                       pi_org_id      in varchar2,
                       po_plan_id     out sccontainerplan.sccp_plan_id%type)
    return varchar2 is
  begin
    begin
      select cp.sccp_plan_id
        into po_plan_id
        from sccontainerplan cp
       where exists (select 0
                from shsailingschedule sh, cshipcanonical ship
               where sh.shss_org_id = pi_org_id
                 and ship.cshc_org_id = pi_org_id
                 and ship.cshc_id = sh.shss_vessel_code
                 and ship.cshc_en_vessel = pi_vessel_name
                 and cp.sccp_voyage_code = sh.shss_voyage_id
                 and sh.shss_exp_voyage_code = pi_voyage
                 and sh.shss_delete_flag = 'N'
                 and sh.shss_cancel_flag = 'N')
         and cp.sccp_cancel_flag = 'N'
         and cp.sccp_plan_type_code = '2'
         and cp.sccp_imp_exp_flag = 'E'
         and cp.sccp_org_id = pi_org_id;
    exception
      when no_data_found then
        return '����:' || pi_vessel_name || '����:' || pi_voyage || '�Ҳ�����ƻ�';
    end;
    return '';
  end get_plan_id;
  
  function checkExist(pi_plan_id  varchar2,
                      pi_blno     varchar2,
                      pi_size     varchar2,
                      pi_type     varchar2,
                      l_cnt_qnt   out number
                      ) return number is
    l_id       SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
  begin
    begin
      select cp.SCPQ_CNT_QNT_ID, cp.scpq_cnt_qnt
        into l_id, l_cnt_qnt
        from SCCONTAINERPLANQUANTITY cp
       where cp.SCPQ_CANCEL_FLAG = 'N'
         and cp.scpq_cnt_size = pi_size
         and cp.scpq_cnt_type = pi_type
         and cp.scpq_bl_no = pi_blno
         and cp.scpq_plan_id = pi_plan_id
         and rownum = 1;
    exception
      when no_data_found then
        l_id := -1;
    end;
    return l_id;
  end checkExist;  

  function checkExist(pi_plan_id  varchar2,
                      pi_blno     varchar2,
                      pi_size     varchar2,
                      pi_type     varchar2,
                      po_eir_flag out boolean --�Ƿ������豸���ӵ�
                      ) return number is
    l_id     SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
    l_eir_id SCEIR.SCEA_CNT_REQ_ID%type;
  begin
    po_eir_flag := false;
    begin
      select cp.SCPQ_CNT_QNT_ID, max(eir.SCEA_CNT_REQ_ID)
        into l_id, l_eir_id
        from SCEIR eir, SCCONTAINERPLANQUANTITY cp
       where cp.SCPQ_CNT_QNT_ID = eir.SCEA_CNT_REQ_ID(+)
         and cp.scpq_plan_id = eir.scea_plan_id(+)
         and cp.SCPQ_CANCEL_FLAG = 'N'
         and cp.scpq_cnt_size = pi_size
         and cp.scpq_cnt_type = pi_type
         and cp.scpq_bl_no = pi_blno
         and cp.scpq_plan_id = pi_plan_id
       group by cp.SCPQ_CNT_QNT_ID;
    
      if l_eir_id > 0 then
        po_eir_flag := true;
      end if;
    exception
      when no_data_found then
        l_id := -1;
    end;
    return l_id;
  end checkExist;

  function checkExist(pi_org_id varchar2, --�ֹ�˾Id
                      pi_blno   varchar2) -- �ᵥ��
   return number is
    l_id SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
  
  begin
  
    begin
      select cp.SCPQ_CNT_QNT_ID
        into l_id
        from SCCONTAINERPLANQUANTITY cp, sccontainerplan cp1
       where cp.scpq_plan_id = cp1.sccp_plan_id
         and cp.SCPQ_CANCEL_FLAG = 'N'
         and cp.scpq_bl_no = pi_blno
         and cp1.sccp_org_id = pi_org_id;
    exception
      when no_data_found then
        l_id := -1;
    end;
    return l_id;
  end checkExist;

  function checkExist(pi_plan_id  varchar2,
                      pi_blno     varchar2,
                      po_eir_flag out boolean --�Ƿ������豸���ӵ�
                      ) return number is
    l_id     SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
    l_eir_id SCEIR.SCEA_CNT_REQ_ID%type;
  begin
    po_eir_flag := false;
    begin
      select cp.SCPQ_CNT_QNT_ID, max(eir.SCEA_CNT_REQ_ID)
        into l_id, l_eir_id
        from SCEIR eir, SCCONTAINERPLANQUANTITY cp
       where cp.SCPQ_CNT_QNT_ID = eir.SCEA_CNT_REQ_ID(+)
         and cp.SCPQ_CANCEL_FLAG = 'N'
         and cp.scpq_bl_no = pi_blno
         and cp.scpq_plan_id = pi_plan_id
       group by cp.SCPQ_CNT_QNT_ID;
    
      if l_eir_id > 0 then
        po_eir_flag := true;
      end if;
    exception
      when no_data_found then
        l_id := -1;
      when too_many_rows then
        l_id := -2;
    end;
    return l_id;
  end checkExist;
  
  --У���ᵥ���ڣ����������Ƿ�һ��
  function checkBLVesselVoyage(pi_org_id   in varchar2,
                               pi_bl_no    in varchar2,
                               pi_vessel   in varchar2,
                               pi_voyage   in varchar2 )return varchar2 is
     v_vessel    cshipcanonical.cshc_en_vessel%type;
     v_voyage    shsailingschedule.shss_exp_voyage_code%type;
     v_message   varchar2(100);
  begin
    begin
      select s.shss_exp_voyage_code ,c.cshc_en_vessel
        into v_voyage , v_vessel
        from sccontainerplanquantity cp, sccontainerplan cp1 ,shsailingschedule s ,cshipcanonical c
       where cp.scpq_plan_id = cp1.sccp_plan_id
         and cp.scpq_cancel_flag = 'N'
         and cp.scpq_bl_no = pi_bl_no
         and cp1.sccp_org_id = pi_org_id
         and cp1.sccp_voyage_code = s.shss_voyage_id
         and s.shss_vessel_code = c.cshc_id;
    exception
      when no_data_found then
        null;
    end;
    if v_vessel <> pi_vessel or v_voyage <> pi_voyage then
      v_message:= pi_bl_no || '�ᵥ���Ѵ��ڣ��Ҵ������β�ͬ';
    end if;
    return v_message;
  end checkBLVesselVoyage;

  procedure updateErrMsg(pi_id varchar2, msg varchar2) is
  begin
    update ictnlistin cl
       set cl.itli_disposeflag = 'E',
           cl.itli_err_desc    =  msg,
           cl.itli_disposetime = sysdate
     where cl.itli_id = pi_id;
  end updateErrMsg;
  
  function getchange --
  (pi_cnt_qnt_id            varchar2,
   pi_cnt_size              varchar2,
   pi_cnt_type              varchar2,
   pi_cnt_qnt               sccontainerplanquantity.scpq_cnt_qnt%type,
   pi_cnt_get_place_code    varchar2,
   pi_discharge_place_code  varchar2,
   pi_destination_port_code varchar2,
   pi_remark                varchar2) return varchar2 is
    l_cnt_size                sccontainerplanquantity.scpq_cnt_size%type;
    l_cnt_type                sccontainerplanquantity.scpq_cnt_type%type;
    l_cnt_qnt                 sccontainerplanquantity.scpq_cnt_qnt%type;
    l_cnt_delivery_place_code sccontainerplanquantity.scpq_cnt_delivery_place_code%type;
    l_discharge_port_code     sccontainerplanquantity.scpq_discharge_port_code%type;
    l_port_destination_code   sccontainerplanquantity.scpq_port_destination_code%type;
    l_remark                  sccontainerplanquantity.scpq_remark%type;
  
    l_error varchar2(300) := '';
  begin
    select nvl(cpq.scpq_cnt_size, '0'),
           nvl(cpq.scpq_cnt_type, '0'),
           nvl(cpq.scpq_cnt_qnt, '0'),
           nvl(cpq.scpq_cnt_delivery_place_code, '0'),
           nvl(cpq.scpq_discharge_port_code, '0'),
           nvl(cpq.scpq_port_destination_code, '0'),
           nvl(cpq.scpq_remark, '0')
      into l_cnt_size,
           l_cnt_type,
           l_cnt_qnt,
           l_cnt_delivery_place_code,
           l_discharge_port_code,
           l_port_destination_code,
           l_remark
      from sccontainerplanquantity cpq
     where cpq.scpq_cnt_qnt_id = pi_cnt_qnt_id;
  
    if nvl(pi_cnt_size, '0') != l_cnt_size then
      l_error := l_error || '�ߴ�:' || l_cnt_size || '-->' || pi_cnt_size;
    end if;
    if nvl(pi_cnt_type, '0') != l_cnt_type then
      l_error := l_error || '����:' || l_cnt_type || '-->' || pi_cnt_type;
    end if;
    
    if nvl(pi_cnt_qnt, '0') != l_cnt_qnt then
      l_error := l_error || '����:' || l_cnt_qnt || '-->' || pi_cnt_qnt;
    end if;    
    
    if nvl(pi_cnt_get_place_code, '0') != l_cnt_delivery_place_code then
      l_error := l_error || '�����:' || l_cnt_delivery_place_code || '-->' ||
                 pi_cnt_get_place_code;
    end if;
    if nvl(pi_discharge_place_code, '0') != l_discharge_port_code then
      l_error := l_error || 'ж����:' || l_discharge_port_code || '-->' ||
                 pi_discharge_place_code;
    end if;
    if nvl(pi_destination_port_code, '0') != l_port_destination_code then
      l_error := l_error || 'Ŀ�ĸ�:' || l_port_destination_code || '-->' ||
                 pi_destination_port_code;
    end if;
  
    if nvl(pi_remark, '0') != l_remark then
      l_error := l_error || '��ע��һ��;';
    end if;
    return l_error;
  exception
    when others then
      return '';
  end getchange;
  
  procedure delCtn(pi_plan_id in varchar2,
                   pi_bl_no   in varchar2) is
  begin
    delete from sccontainerplanquantity scpq where scpq.scpq_plan_id = pi_plan_id and scpq.scpq_bl_no = pi_bl_no and scpq.scpq_cancel_flag = 'N';
  end delCtn;
  
  /* ���׸�ͨ���ͱ���
   pi_opt_type�������ͣ�0��ѯ 1���� 2�޸� 3ɾ��
   */
  procedure handleEirSendSchedule(pi_edi_id   in varchar2,
                                  pi_user_id  in varchar2,
                                  pi_org_id   in varchar2,
                                  pi_opt_type in varchar2)is
    l_paramsValue     saparamsdetail%rowtype;
  begin
    getSaparamsConfig('SACO_CNTREQ_IMPORT_EIR_SEND_SCHEDULE',pi_org_id, l_paramsValue);
    if l_paramsValue.Sapd_Value1 = 'Y' and instr(l_paramsValue.Sapd_Value2,pi_opt_type) > 0 then
      insert into ieeirsendschedule
        (eess_send_schedule_id,
         eess_supplier_code,
         eess_service_name,
         eess_bl_no,
         eess_cnt_no,
         eess_org_id,
         eess_creator,
         eess_create_time,
         eess_operation_type,
         eess_modifier,
         eess_modify_time)
      select seq_eess_send_schedule_id.nextval,
             'ZJYGT',
             'EEIR',
             i.itli_blno,
             i.itli_ctnno,
             i.itli_orgid,
             pi_user_id,
             sysdate,
             pi_opt_type,
             pi_user_id,
             sysdate
        from ictnlistin i
       where i.itli_id = pi_edi_id;
    end if;
  end handleEirSendSchedule;
  
  
   /************************************
  *
  * ��������  
  *************************************/
  procedure importAnother(pi_batch_no in varchar2,
                          pi_user_id  in varchar2,
                          pi_import_mode in varchar2,
                          pi_check_vv    in varchar2) is
    l_creat_time date := sysdate;
    l_plan_id    sccontainerplan.sccp_plan_id%type := 0;
    l_check_msg  ictnlistin.itli_err_desc%type := '';
    l_edi_id     ictnlistin.itli_id%type;
  
    l_cnt_size              ccontainermeasure.cctm_cnt_size%type;
    l_cnt_type              ccontainermeasure.cctm_cnt_type%type;
    l_ctn_status            ictnlistin.itli_ctn_status%type;
    l_cnt_quentity          ictnlistin.itli_cnt_quentity%type;
    l_blno                  ictnlistin.itli_blno%type;
    l_soc                   ictnlistin.itli_soc%type;
    l_destination_port_code ictnlistin.itli_destination_port_code%type;
    l_remark                ictnlistin.itli_remark%type;
    l_ctnoperator_code      ictnlistin.itli_ctnoperator_code%type;
    l_cnt_get_place_code    ictnlistin.itli_cnt_get_place_code%type;
    l_cnt_return_place_code ictnlistin.itli_cnt_return_place_code%type;
    l_cnt_get_validate      ictnlistin.itli_cnt_get_validate%type;
    l_cnt_rtn_validate      ictnlistin.itli_cnt_rtn_validate%type;
    l_shipper               ictnlistin.itli_shipper%type;
    l_discharge_place_code  ictnlistin.itli_discharge_place_code%type;
    l_temerature_setting    ictnlistin.itli_temerature_setting%type;
  
    l_vessel_name ictnlistin.itli_vessel_name%type;
    l_voyage      ictnlistin.itli_voyage%type;
    l_org_id      ictnlistin.itli_orgid%type;
    cur_ctn_list  edi_util_pkg.ref_cursor_type;
  
    l_msg_type ictnlistin.itli_messagetype%type;
  
    l_cnt_qnt_id      SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
    l_eir_flag        boolean;
    l_haulier_code    ictnlistin.itli_haulier_code%type;
    l_record_type     ictnlistin.itli_record_type%type;
    l_count           int := 0;
    l_count_eir       int := 0;
  begin
    if nvl(pi_batch_no, 'null') = 'null' then
      open cur_ctn_list for
        select cl.itli_id,
               cm.cctm_cnt_size,
               cm.cctm_cnt_type,
               cl.itli_ctn_status,
               cl.itli_cnt_quentity,
               cl.itli_blno,
               cl.itli_soc,
               cl.itli_destination_port_code,
               cl.itli_remark,
               cl.itli_ctnoperator_code,
               cl.itli_cnt_get_place_code,
               cl.itli_cnt_return_place_code,
               cl.itli_cnt_get_validate,
               cl.itli_cnt_rtn_validate,
               cl.itli_shipper,
               cl.itli_discharge_place_code,
               cl.itli_temerature_setting,
               cl.itli_vessel_name,
               cl.itli_voyage,
               cl.itli_orgid,
               cl.itli_messagetype,
               cl.itli_haulier_code,
               cl.itli_record_type
          from ictnlistin cl, ccontainermeasure cm, temp_id t
         where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
           and cl.itli_disposeflag <> 'D' --����ɾ��״̬
           and cl.itli_id = t.id;
    else
      open cur_ctn_list for
        select cl.itli_id,
               cm.cctm_cnt_size,
               cm.cctm_cnt_type,
               cl.itli_ctn_status,
               cl.itli_cnt_quentity,
               cl.itli_blno,
               cl.itli_soc,
               cl.itli_destination_port_code,
               cl.itli_remark,
               cl.itli_ctnoperator_code,
               cl.itli_cnt_get_place_code,
               cl.itli_cnt_return_place_code,
               cl.itli_cnt_get_validate,
               cl.itli_cnt_rtn_validate,
               cl.itli_shipper,
               cl.itli_discharge_place_code,
               cl.itli_temerature_setting,
               cl.itli_vessel_name,
               cl.itli_voyage,
               cl.itli_orgid,
               cl.itli_messagetype,
               cl.itli_haulier_code,
               cl.itli_record_type
          from ictnlistin cl, ccontainermeasure cm
         where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
           and cl.itli_disposeflag <> 'D' --����ɾ��״̬
           and cl.itli_batchno = pi_batch_no
         order by cl.itli_id;
    end if;

    delete from temp_exp_mf;                                                
    loop
      <<nextRecord>>
      l_check_msg := ''; --reset
      l_eir_flag  := false;
      l_cnt_qnt_id := null;
      l_count_eir := 0;
      fetch cur_ctn_list
        into l_edi_id, l_cnt_size, l_cnt_type, l_ctn_status, 
             l_cnt_quentity, l_blno, l_soc, l_destination_port_code, 
             l_remark, l_ctnoperator_code, l_cnt_get_place_code, 
             l_cnt_return_place_code, l_cnt_get_validate, l_cnt_rtn_validate, 
             l_shipper, l_discharge_place_code, l_temerature_setting, l_vessel_name, 
             l_voyage, l_org_id, l_msg_type, l_haulier_code, l_record_type;
    
      exit when cur_ctn_list%NOTFOUND;
      
        --��������������ɾ��
        if l_record_type = 1 and l_org_id = '5072' then
          goto nextRecord;
        end if;
    
    
        if (nvl(length(l_vessel_name), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_voyage), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_check_msg), 0) = 0) then
          --����������ζ���Ϊ�� ��ʼ����ƻ�
          l_check_msg := get_plan_id(l_vessel_name,
                                     l_voyage,
                                     l_org_id,
                                     l_plan_id);
        end if;
      
        if (nvl(length(l_cnt_type), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_cnt_size), 0) = 0) then
          l_check_msg := l_check_msg || '�ߴ�Ϊ��;';
        end if;
            
        if l_check_msg is not null then
          updateErrMsg(l_edi_id, l_check_msg);
          goto nextRecord;
        end if;

      savepoint save1;
      
      begin
        select count(0)
          into l_count
          from sccontainerplanquantity scct
         where scct.scpq_bl_no = l_blno
           and (nvl(pi_check_vv,'N') = 'N' 
                or (pi_check_vv = 'Y' and scct.scpq_plan_id = l_plan_id))
           and scct.scpq_cancel_flag = 'N';
      end;
      
      begin
        select count(0)
          into l_count_eir
          from sceir s
         where s.scea_bl_no = l_blno
           and (nvl(pi_check_vv,'N') = 'N'
                 or (pi_check_vv = 'Y' and s.scea_plan_id = l_plan_id))
           and s.scea_cancel_flag = 'N'
           and s.scea_org_id = l_org_id;
      end;
    
      begin
      
        if pi_import_mode in ('1','2') then
          
          if l_count_eir > 0 then
            update sceir s
               set s.scea_cancel_flag = 'Y',
                   s.scea_cancel_time = sysdate,
                   s.scea_cancel_type = '2'
             where s.scea_bl_no = l_blno
               and (nvl(pi_check_vv,'N') = 'N'
                     or (pi_check_vv = 'Y' and s.scea_plan_id = l_plan_id))
               and s.scea_cancel_flag = 'N'
               and s.scea_org_id = l_org_id;
          end if;
          
          if l_count > 0 then
            update sccontainerplanquantity scct
               set scct.scpq_cancel_flag = 'Y'
             where scct.scpq_bl_no = l_blno
               and (nvl(pi_check_vv,'N') = 'N' 
                     or (pi_check_vv = 'Y' and scct.scpq_plan_id = l_plan_id))
               and scct.scpq_cancel_flag = 'N'
               and scct.scpq_cnt_qnt_id not in (select id from temp_charid);
          end if;
       
        --0:��������  1:ɾ������  2:���ǵ���   
        if nvl(l_record_type, 0) in (0,2) then
          select seq_scpq_cnt_qnt_id.nextval into l_cnt_qnt_id from dual; 
          insert into temp_charid(id) values(l_cnt_qnt_id);
          insert into sccontainerplanquantity cpq
            (cpq.scpq_cnt_qnt_id,
             cpq.scpq_plan_id,
             cpq.scpq_cnt_size,
             cpq.scpq_cnt_type,
             cpq.scpq_cnt_status,
             cpq.scpq_cnt_qnt,
             cpq.scpq_bl_no,
             cpq.scpq_soc_flag,
             cpq.scpq_port_destination_code,
             cpq.scpq_remark,
             cpq.scpq_cnt_operator_code,
             cpq.scpq_creator,
             cpq.scpq_creat_time,
             cpq.scpq_cnt_delivery_place_code,
             cpq.scpq_cnt_return_place_code,
             cpq.scpq_delivery_time_start,
             cpq.scpq_delivery_time_end,
             cpq.scpq_cnt_user_name, -- ������
             cpq.scpq_discharge_port_code,
             cpq.scpq_tempeature_vent,
             cpq.scpq_data_source,
             cpq.SCPQ_CARRIER_CONFIRM,
             cpq.scpq_haulier_code)
          values
            (l_cnt_qnt_id,
             l_plan_id,
             l_cnt_size,
             l_cnt_type,
             l_ctn_status,
             l_cnt_quentity,
             l_blno,
             nvl(l_soc,'N'),
             l_destination_port_code,
             l_remark,
             l_ctnoperator_code,
             pi_user_id,
             l_creat_time,
             l_cnt_get_place_code,
             l_cnt_return_place_code,
             l_cnt_get_validate,
             l_cnt_rtn_validate,
             l_shipper,
             l_discharge_place_code,
             l_temerature_setting,
             '1',
             decode(l_msg_type, 'XG00006501', 'Y'), --������ȷ�ϵ���
             l_haulier_code);
             update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_quentity || 'X' || l_cnt_size || l_cnt_type || ',��������ɹ�' where cl.itli_id = l_edi_id;           
        elsif l_count = 0 and l_record_type = 1 then     
              updateErrMsg(l_edi_id, '�ᵥ��:'|| l_blno || '�޴�������,�޷�ɾ��!;');
        elsif l_count > 0 and  l_record_type = 1 and pi_import_mode <> '2' then
              update sccontainerplanquantity cpq 
                 set cpq.scpq_cancel_flag = 'Y',
                     cpq.scpq_modifier = pi_user_id,
                     cpq.scpq_modify_time = sysdate
               where cpq.scpq_plan_id = l_plan_id
                 and cpq.scpq_bl_no = l_blno;
              handleEirSendSchedule(l_edi_id,pi_user_id,l_org_id,'3');--���׸�ͨ���ͱ���
              update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_size || l_cnt_type || ',ɾ������ɹ�:' where cl.itli_id = l_edi_id;
        end if;
        end if;
        
        update ictnlistin cl
           set cl.itli_disposeflag = 'Y',
               cl.itli_disposetime = sysdate
         where cl.itli_id = l_edi_id;
      exception
        when others then
          rollback to save1;
          updateErrMsg(l_edi_id, SQLERRM);
      end;
    end loop;
    close cur_ctn_list;
  end importAnother;

 /***************************
  ������EC���ĵ���
  */
  procedure importEC(pi_batch_no in varchar2, -- ���κ�
                     pi_user_id  in varchar2 -- ������Id
                    ) is
      l_creat_time date := sysdate;
      l_plan_id    sccontainerplan.sccp_plan_id%type := 0;
      l_check_msg  ictnlistin.itli_err_desc%type := '';
      l_edi_id     ictnlistin.itli_id%type;

      l_cnt_size              ccontainermeasure.cctm_cnt_size%type;
      l_cnt_type              ccontainermeasure.cctm_cnt_type%type;
      l_ctn_status            ictnlistin.itli_ctn_status%type;
      l_cnt_quentity          ictnlistin.itli_cnt_quentity%type;
      l_blno                  ictnlistin.itli_blno%type;
      l_soc                   ictnlistin.itli_soc%type;
      l_destination_port_code ictnlistin.itli_destination_port_code%type;
      l_remark                ictnlistin.itli_remark%type;
      l_ctnoperator_code      ictnlistin.itli_ctnoperator_code%type;
      l_cnt_get_place_code    ictnlistin.itli_cnt_get_place_code%type;
      l_cnt_return_place_code ictnlistin.itli_cnt_return_place_code%type;
      l_cnt_get_validate      ictnlistin.itli_cnt_get_validate%type;
      l_cnt_rtn_validate      ictnlistin.itli_cnt_rtn_validate%type;
      l_shipper               ictnlistin.itli_shipper%type;
      l_discharge_place_code  ictnlistin.itli_discharge_place_code%type;
      l_temerature_setting    ictnlistin.itli_temerature_setting%type;

      l_vessel_name ictnlistin.itli_vessel_name%type;
      l_voyage      ictnlistin.itli_voyage%type;
      l_org_id      ictnlistin.itli_orgid%type;
      cur_ctn_list  edi_util_pkg.ref_cursor_type;

      l_msg_type ictnlistin.itli_messagetype%type;

      l_cnt_qnt_id      SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
      l_eir_flag        boolean;
      l_cnt_qnt         SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT%type;
      l_haulier_code    ictnlistin.itli_haulier_code%type;
      l_record_type     ictnlistin.itli_record_type%type;
      l_check_bl_voyage cbizconfig.cbic_sc_value%type;
      l_change ictnlistin.itli_err_desc%type;
      l_count           int := 0;
      l_paramsValue     saparamsdetail%rowtype;

      v_exp_cntreq_import_rule saparamsdetail%rowtype;
  begin
      if nvl(pi_batch_no, 'null') = 'null' then
          open cur_ctn_list for
              select cl.itli_id,
                     cm.cctm_cnt_size,
                     cm.cctm_cnt_type,
                     cl.itli_ctn_status,
                     cl.itli_cnt_quentity,
                     cl.itli_blno,
                     cl.itli_soc,
                     cl.itli_destination_port_code,
                     cl.itli_remark,
                     cl.itli_ctnoperator_code,
                     cl.itli_cnt_get_place_code,
                     cl.itli_cnt_return_place_code,
                     cl.itli_cnt_get_validate,
                     cl.itli_cnt_rtn_validate,
                     cl.itli_shipper,
                     cl.itli_discharge_place_code,
                     cl.itli_temerature_setting,
                     cl.itli_vessel_name,
                     cl.itli_voyage,
                     cl.itli_orgid,
                     cl.itli_messagetype,
                     cl.itli_haulier_code,
                     cl.itli_record_type
              from ictnlistin cl, ccontainermeasure cm, temp_id t
              where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
                and cl.itli_disposeflag <> 'D' --����ɾ��״̬
                and cl.itli_id = t.id;
      else
          open cur_ctn_list for
              select cl.itli_id,
                     cm.cctm_cnt_size,
                     cm.cctm_cnt_type,
                     cl.itli_ctn_status,
                     cl.itli_cnt_quentity,
                     cl.itli_blno,
                     cl.itli_soc,
                     cl.itli_destination_port_code,
                     cl.itli_remark,
                     cl.itli_ctnoperator_code,
                     cl.itli_cnt_get_place_code,
                     cl.itli_cnt_return_place_code,
                     cl.itli_cnt_get_validate,
                     cl.itli_cnt_rtn_validate,
                     cl.itli_shipper,
                     cl.itli_discharge_place_code,
                     cl.itli_temerature_setting,
                     cl.itli_vessel_name,
                     cl.itli_voyage,
                     cl.itli_orgid,
                     cl.itli_messagetype,
                     cl.itli_haulier_code,
                     cl.itli_record_type
              from ictnlistin cl, ccontainermeasure cm
              where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
                and cl.itli_disposeflag <> 'D' --����ɾ��״̬
                and cl.itli_batchno = pi_batch_no
              order by cl.itli_id;
      end if;
      begin
          select i.itli_orgid
          into l_org_id
          from ictnlistin i
          where i.itli_batchno = pi_batch_no
            and i.itli_orgid is not null
            and rownum = 1;
      exception
          when no_data_found then
              null;
      end;

      getSaparamsConfig('SACO_EXP_CNTREQ_IMPORT_MODE',l_org_id, v_exp_cntreq_import_rule);
      if v_exp_cntreq_import_rule.sapd_value1 = 'Y' then
          if cur_ctn_list%isopen then
              close cur_ctn_list;
          end if;
          importAnother(pi_batch_no, pi_user_id, v_exp_cntreq_import_rule.sapd_value2,v_exp_cntreq_import_rule.sapd_value3);
          return;
      end if;

      l_check_bl_voyage := edi_util_pkg.get_bizconfig('SACO_EXP_CNT_IMPORT_CONTROL',
                                                      l_org_id,
                                                      '2',
                                                      'N');
      --��ȡ�����������
      getSaparamsConfig('SACO_EXP_CNTREQ_IMPORT_CHECK_RULE',l_org_id, l_paramsValue);

      delete from temp_exp_mf;
      loop
          <<nextRecord>>
              l_check_msg := ''; --reset
          l_eir_flag  := false;
          l_cnt_qnt   := null;
          fetch cur_ctn_list
              into l_edi_id, l_cnt_size, l_cnt_type, l_ctn_status, l_cnt_quentity, l_blno, l_soc, l_destination_port_code, l_remark, l_ctnoperator_code, l_cnt_get_place_code, l_cnt_return_place_code, l_cnt_get_validate, l_cnt_rtn_validate, l_shipper, l_discharge_place_code, l_temerature_setting, l_vessel_name, l_voyage, l_org_id, l_msg_type, l_haulier_code, l_record_type;

          exit when cur_ctn_list%NOTFOUND;

          updateErrMsg(l_edi_id, '');

          if l_msg_type = 'XG00006501' and l_org_id = '222' then
              l_cnt_qnt_id := checkExist(l_org_id, l_blno);
              if l_cnt_qnt_id = -1 then
                  updateErrMsg(l_edi_id,
                               '�ᵥ��Ϊ:' || l_blno || '�������󲻴���;');

              else
                  update sccontainerplanquantity cpq
                  set cpq.scpq_cnt_operator_code = nvl(l_ctnoperator_code,
                                                       cpq.scpq_cnt_operator_code),

                      cpq.SCPQ_CARRIER_CONFIRM = 'Y'
                  where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
              end if;
              goto nextRecord;
          else
              if (nvl(length(l_vessel_name), 0) = 0) then
                  l_check_msg := l_check_msg || '����Ϊ��;';
              end if;

              if (nvl(length(l_voyage), 0) = 0) then
                  l_check_msg := l_check_msg || '����Ϊ��;';
              end if;

              if (nvl(length(l_check_msg), 0) = 0) then
                  --����������ζ���Ϊ�� ��ʼ����ƻ�
                  l_check_msg := get_plan_id(l_vessel_name,
                                             l_voyage,
                                             l_org_id,
                                             l_plan_id);
              end if;

              if (nvl(length(l_cnt_type), 0) = 0) then
                  l_check_msg := l_check_msg || '����Ϊ��;';
              end if;

              if (nvl(length(l_cnt_size), 0) = 0) then
                  l_check_msg := l_check_msg || '�ߴ�Ϊ��;';
              end if;
              if (nvl(length(l_check_msg), 0) = 0) then
                  if l_org_id in ('439','222','4392') then
                      l_cnt_qnt_id := checkExist(l_plan_id, l_blno, l_eir_flag);
                      if l_cnt_qnt_id = -2 then
                          updateErrMsg(l_edi_id,
                                       '���ڶ���ᵥ����ͬ��������:' || l_blno);
                          goto nextRecord;
                      end if;
                  elsif l_org_id = '86' then  --TD34208
                      l_cnt_qnt_id := checkExist(l_plan_id,
                                                 l_blno,
                                                 l_cnt_size,
                                                 l_cnt_type,
                                                 l_cnt_qnt);
                  else

                      l_cnt_qnt_id := checkExist(l_plan_id,
                                                 l_blno,
                                                 l_cnt_size,
                                                 l_cnt_type,
                                                 l_eir_flag);
                  end if;
              else
                  updateErrMsg(l_edi_id, l_check_msg);
                  goto nextRecord;
              end if;
          end if;
          savepoint save1;

          begin
              if l_eir_flag then
                  if l_record_type = 1 then -- ɾ��
                      updateErrMsg(l_edi_id,'������EIR:������ɾ��ʧ�ܡ�');
                  else
                      updateErrMsg(l_edi_id,
                                   '������EIR:' || getchange(l_cnt_qnt_id,
                                                             l_cnt_size,
                                                             l_cnt_type,
                                                             l_cnt_quentity,
                                                             l_cnt_get_place_code,
                                                             l_discharge_place_code,
                                                             l_destination_port_code,
                                                             l_remark));
                  end if;
                  goto nextRecord;
              end if;

              --У���ᵥ���ڣ����������Ƿ�һ��
              if l_paramsValue.Sapd_Value1 = '1' then
                  l_check_msg := checkBLVesselVoyage(l_org_id,l_blno,l_vessel_name,l_voyage);
                  if l_check_msg is not null then
                      updateErrMsg(l_edi_id, l_check_msg);
                      goto nextRecord;
                  end if;
              end if;

              if l_cnt_qnt_id != -1 and l_check_bl_voyage = 'Y' and nvl(l_record_type,0) = 0 and l_org_id != '86' then
                  updateErrMsg(l_edi_id, '�ú����Ѵ����ᵥ����ͬ��������;');
                  goto nextRecord;
              end if;

              --TD34208,0:��������  1:ɾ������  2:���ǵ���
              if l_cnt_qnt_id = -1 and nvl(l_record_type, 0) = 0 then
                  insert into sccontainerplanquantity cpq
                  (cpq.scpq_cnt_qnt_id,
                   cpq.scpq_plan_id,
                   cpq.scpq_cnt_size,
                   cpq.scpq_cnt_type,
                   cpq.scpq_cnt_status,
                   cpq.scpq_cnt_qnt,
                   cpq.scpq_bl_no,
                   cpq.scpq_soc_flag,
                   cpq.scpq_port_destination_code,
                   cpq.scpq_remark,
                   cpq.scpq_cnt_operator_code,
                   cpq.scpq_creator,
                   cpq.scpq_creat_time,
                   cpq.scpq_cnt_delivery_place_code,
                   cpq.scpq_cnt_return_place_code,
                   cpq.scpq_delivery_time_start,
                   cpq.scpq_delivery_time_end,
                   cpq.scpq_cnt_user_name, -- ������
                   cpq.scpq_discharge_port_code,
                   cpq.scpq_tempeature_vent,
                   cpq.scpq_data_source,
                   cpq.SCPQ_CARRIER_CONFIRM,
                   cpq.scpq_haulier_code)
                  values
                      (seq_scpq_cnt_qnt_id.nextval,
                       l_plan_id,
                       l_cnt_size,
                       l_cnt_type,
                       l_ctn_status,
                       l_cnt_quentity,
                       l_blno,
                       nvl(l_soc,'N'),
                       l_destination_port_code,
                       l_remark,
                       l_ctnoperator_code,
                       pi_user_id,
                       l_creat_time,
                       l_cnt_get_place_code,
                       l_cnt_return_place_code,
                       l_cnt_get_validate,
                       l_cnt_rtn_validate,
                       l_shipper,
                       l_discharge_place_code,
                       l_temerature_setting,
                       '1',
                       decode(l_msg_type, 'XG00006501', 'Y'), --������ȷ�ϵ���
                       l_haulier_code);
                  update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_quentity || 'X' || l_cnt_size || l_cnt_type || ',��������ɹ�' where cl.itli_id = l_edi_id;
              elsif l_record_type = 2 then --���ǵ�����ɾ�����ٵ���
                  select count(1) into l_count from temp_exp_mf where SSEM_BL_NO = l_blno;
                  if l_count = 0 then
                      delCtn(l_plan_id, l_blno);
                      insert into temp_exp_mf (SSEM_BL_NO) values (l_blno);
                  end if;
                  insert into sccontainerplanquantity cpq
                  (cpq.scpq_cnt_qnt_id,
                   cpq.scpq_plan_id,
                   cpq.scpq_cnt_size,
                   cpq.scpq_cnt_type,
                   cpq.scpq_cnt_status,
                   cpq.scpq_cnt_qnt,
                   cpq.scpq_bl_no,
                   cpq.scpq_soc_flag,
                   cpq.scpq_port_destination_code,
                   cpq.scpq_remark,
                   cpq.scpq_cnt_operator_code,
                   cpq.scpq_creator,
                   cpq.scpq_creat_time,
                   cpq.scpq_cnt_delivery_place_code,
                   cpq.scpq_cnt_return_place_code,
                   cpq.scpq_delivery_time_start,
                   cpq.scpq_delivery_time_end,
                   cpq.scpq_cnt_user_name, -- ������
                   cpq.scpq_discharge_port_code,
                   cpq.scpq_tempeature_vent,
                   cpq.scpq_data_source,
                   cpq.SCPQ_CARRIER_CONFIRM,
                   cpq.scpq_haulier_code)
                  values
                      (seq_scpq_cnt_qnt_id.nextval,
                       l_plan_id,
                       l_cnt_size,
                       l_cnt_type,
                       l_ctn_status,
                       l_cnt_quentity,
                       l_blno,
                       nvl(l_soc,'N'),
                       l_destination_port_code,
                       l_remark,
                       l_ctnoperator_code,
                       pi_user_id,
                       l_creat_time,
                       l_cnt_get_place_code,
                       l_cnt_return_place_code,
                       l_cnt_get_validate,
                       l_cnt_rtn_validate,
                       l_shipper,
                       l_discharge_place_code,
                       l_temerature_setting,
                       '1',
                       decode(l_msg_type, 'XG00006501', 'Y'), --������ȷ�ϵ���
                       l_haulier_code);
                  update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_quentity || 'X' || l_cnt_size || l_cnt_type || ',���ǵ���ɹ�' where cl.itli_id = l_edi_id;
              elsif l_cnt_qnt_id = -1 and l_record_type = 1 then
                  updateErrMsg(l_edi_id, '�ᵥ��:'|| l_blno || ',���ͳߴ�:' || l_cnt_size || l_cnt_type || ',�޴�������,�޷�ɾ��!;');
              elsif l_cnt_qnt_id > 0 and  l_record_type = 1 then
                  delete from sccontainerplanquantity cpq where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
                  update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_qnt || 'X' || l_cnt_size || l_cnt_type || ',ɾ������ɹ�:' where cl.itli_id = l_edi_id;
              else
                  l_change := getchange(l_cnt_qnt_id,
                                        l_cnt_size,
                                        l_cnt_type,
                                        l_cnt_quentity,
                                        l_cnt_get_place_code,
                                        l_discharge_place_code,
                                        l_destination_port_code,
                                        l_remark);
                  update ictnlistin cl
                  set cl.itli_err_desc = '���µ���ɹ�:' || l_change
                  where cl.itli_id = l_edi_id;

                  update sccontainerplanquantity cpq
                  set cpq.scpq_plan_id                 = nvl(l_plan_id,
                                                             cpq.scpq_plan_id),
                      cpq.scpq_cnt_size                = nvl(l_cnt_size,
                                                             cpq.scpq_cnt_size),
                      cpq.scpq_cnt_type                = nvl(l_cnt_type,
                                                             cpq.scpq_cnt_type),
                      cpq.scpq_cnt_status              = nvl(l_ctn_status,
                                                             cpq.scpq_cnt_status),
                      cpq.scpq_cnt_qnt                 = nvl(l_cnt_quentity,
                                                             cpq.scpq_cnt_qnt),
                      cpq.scpq_bl_no                   = nvl(l_blno,
                                                             cpq.scpq_bl_no),
                      cpq.scpq_soc_flag                = nvl(l_soc,
                                                             cpq.scpq_soc_flag),
                      cpq.scpq_port_destination_code   = nvl(l_destination_port_code,
                                                             cpq.scpq_port_destination_code),
                      cpq.scpq_remark                  = nvl(l_remark,
                                                             cpq.scpq_remark),
                      cpq.scpq_cnt_operator_code       = nvl(l_ctnoperator_code,
                                                             cpq.scpq_cnt_operator_code),
                      cpq.scpq_cnt_delivery_place_code = nvl(l_cnt_get_place_code,
                                                             cpq.scpq_cnt_delivery_place_code),
                      cpq.scpq_cnt_return_place_code   = nvl(l_cnt_return_place_code,
                                                             cpq.scpq_cnt_return_place_code),
                      cpq.scpq_delivery_time_start     = nvl(l_cnt_get_validate,
                                                             cpq.scpq_delivery_time_start),
                      cpq.scpq_delivery_time_end       = nvl(l_cnt_rtn_validate,
                                                             cpq.scpq_delivery_time_end),
                      cpq.scpq_cnt_user_name           = nvl(l_shipper,
                                                             cpq.scpq_cnt_user_name), -- ������
                      cpq.scpq_discharge_port_code     = nvl(l_discharge_place_code,
                                                             cpq.scpq_discharge_port_code),
                      cpq.scpq_tempeature_vent         = nvl(l_temerature_setting,
                                                             cpq.scpq_tempeature_vent),
                      cpq.scpq_data_source             = '1',
                      cpq.scpq_haulier_code            = nvl(l_haulier_code,
                                                             cpq.scpq_haulier_code)
                  where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
              end if;
              update ictnlistin cl
              set cl.itli_disposeflag = 'Y'
              where cl.itli_id = l_edi_id;
          exception
              when others then
                  rollback to save1;
                  updateErrMsg(l_edi_id, SQLERRM);
          end;
          --commit; --HY3-838
      end loop;
      close cur_ctn_list;--HY3-838
  end importEC;


 /************************************
  *
  * ��������  
  * ���� ����� 2007-6-7
  * ���� @pi_batch_no �������κ�
  *      @pi_user_id ������id
  * �޸���ʷ
  * 
  *************************************/
  procedure import(pi_batch_no in varchar2, -- ���κ�
                   pi_user_id  in varchar2 -- ������Id
                   ) is
    l_creat_time date := sysdate;
    l_plan_id    sccontainerplan.sccp_plan_id%type := 0;
    l_check_msg  ictnlistin.itli_err_desc%type := '';
    l_edi_id     ictnlistin.itli_id%type;
  
    l_cnt_size              ccontainermeasure.cctm_cnt_size%type;
    l_cnt_type              ccontainermeasure.cctm_cnt_type%type;
    l_ctn_status            ictnlistin.itli_ctn_status%type;
    l_cnt_quentity          ictnlistin.itli_cnt_quentity%type;
    l_blno                  ictnlistin.itli_blno%type;
    l_soc                   ictnlistin.itli_soc%type;
    l_destination_port_code ictnlistin.itli_destination_port_code%type;
    l_remark                ictnlistin.itli_remark%type;
    l_ctnoperator_code      ictnlistin.itli_ctnoperator_code%type;
    l_cnt_get_place_code    ictnlistin.itli_cnt_get_place_code%type;
    l_cnt_return_place_code ictnlistin.itli_cnt_return_place_code%type;
    l_cnt_get_validate      ictnlistin.itli_cnt_get_validate%type;
    l_cnt_rtn_validate      ictnlistin.itli_cnt_rtn_validate%type;
    l_shipper               ictnlistin.itli_shipper%type;
    l_discharge_place_code  ictnlistin.itli_discharge_place_code%type;
    l_temerature_setting    ictnlistin.itli_temerature_setting%type;
  
    l_vessel_name ictnlistin.itli_vessel_name%type;
    l_voyage      ictnlistin.itli_voyage%type;
    l_org_id      ictnlistin.itli_orgid%type;
    cur_ctn_list  edi_util_pkg.ref_cursor_type;
  
    l_msg_type ictnlistin.itli_messagetype%type;
  
    l_cnt_qnt_id      SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT_ID%type;
    l_eir_flag        boolean;
    l_cnt_qnt         SCCONTAINERPLANQUANTITY.SCPQ_CNT_QNT%type;
    l_haulier_code    ictnlistin.itli_haulier_code%type;
    l_record_type     ictnlistin.itli_record_type%type;
    l_check_bl_voyage cbizconfig.cbic_sc_value%type;
    l_change ictnlistin.itli_err_desc%type;
    l_count           int := 0;
    l_paramsValue     saparamsdetail%rowtype;
    
    v_exp_cntreq_import_rule saparamsdetail%rowtype;
  begin
    if nvl(pi_batch_no, 'null') = 'null' then
      open cur_ctn_list for
        select cl.itli_id,
               cm.cctm_cnt_size,
               cm.cctm_cnt_type,
               cl.itli_ctn_status,
               cl.itli_cnt_quentity,
               cl.itli_blno,
               cl.itli_soc,
               cl.itli_destination_port_code,
               cl.itli_remark,
               cl.itli_ctnoperator_code,
               cl.itli_cnt_get_place_code,
               cl.itli_cnt_return_place_code,
               cl.itli_cnt_get_validate,
               cl.itli_cnt_rtn_validate,
               cl.itli_shipper,
               cl.itli_discharge_place_code,
               cl.itli_temerature_setting,
               cl.itli_vessel_name,
               cl.itli_voyage,
               cl.itli_orgid,
               cl.itli_messagetype,
               cl.itli_haulier_code,
               cl.itli_record_type
          from ictnlistin cl, ccontainermeasure cm, temp_id t
         where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
           and cl.itli_disposeflag <> 'D' --����ɾ��״̬
           and cl.itli_id = t.id;
    else
      open cur_ctn_list for
        select cl.itli_id,
               cm.cctm_cnt_size,
               cm.cctm_cnt_type,
               cl.itli_ctn_status,
               cl.itli_cnt_quentity,
               cl.itli_blno,
               cl.itli_soc,
               cl.itli_destination_port_code,
               cl.itli_remark,
               cl.itli_ctnoperator_code,
               cl.itli_cnt_get_place_code,
               cl.itli_cnt_return_place_code,
               cl.itli_cnt_get_validate,
               cl.itli_cnt_rtn_validate,
               cl.itli_shipper,
               cl.itli_discharge_place_code,
               cl.itli_temerature_setting,
               cl.itli_vessel_name,
               cl.itli_voyage,
               cl.itli_orgid,
               cl.itli_messagetype,
               cl.itli_haulier_code,
               cl.itli_record_type
          from ictnlistin cl, ccontainermeasure cm
         where cm.cctm_cnt_code(+) = cl.itli_ctnsizetype_code
           and cl.itli_disposeflag <> 'D' --����ɾ��״̬
           and cl.itli_batchno = pi_batch_no
         order by cl.itli_id;
    end if;
    begin
      select i.itli_orgid
        into l_org_id
        from ictnlistin i
       where i.itli_batchno = pi_batch_no
         and i.itli_orgid is not null
         and rownum = 1;
    exception
      when no_data_found then
        null;
    end;
    
    getSaparamsConfig('SACO_EXP_CNTREQ_IMPORT_MODE',l_org_id, v_exp_cntreq_import_rule);
    if v_exp_cntreq_import_rule.sapd_value1 = 'Y' then 
      if cur_ctn_list%isopen then
        close cur_ctn_list;
      end if;
      importAnother(pi_batch_no, pi_user_id, v_exp_cntreq_import_rule.sapd_value2,v_exp_cntreq_import_rule.sapd_value3);
      return;
    end if;
    
    l_check_bl_voyage := edi_util_pkg.get_bizconfig('SACO_EXP_CNT_IMPORT_CONTROL',
                                                    l_org_id,
                                                    '2',
                                                    'N');
    --��ȡ�����������                                                
    getSaparamsConfig('SACO_EXP_CNTREQ_IMPORT_CHECK_RULE',l_org_id, l_paramsValue);
    
    delete from temp_exp_mf;                                                
    loop
      <<nextRecord>>
      l_check_msg := ''; --reset
      l_eir_flag  := false;
      l_cnt_qnt   := null;
      fetch cur_ctn_list
        into l_edi_id, l_cnt_size, l_cnt_type, l_ctn_status, l_cnt_quentity, l_blno, l_soc, l_destination_port_code, l_remark, l_ctnoperator_code, l_cnt_get_place_code, l_cnt_return_place_code, l_cnt_get_validate, l_cnt_rtn_validate, l_shipper, l_discharge_place_code, l_temerature_setting, l_vessel_name, l_voyage, l_org_id, l_msg_type, l_haulier_code, l_record_type;
    
      exit when cur_ctn_list%NOTFOUND;
    
      updateErrMsg(l_edi_id, '');
    
      if l_msg_type = 'XG00006501' and l_org_id = '222' then
        l_cnt_qnt_id := checkExist(l_org_id, l_blno);
        if l_cnt_qnt_id = -1 then
          updateErrMsg(l_edi_id,
                       '�ᵥ��Ϊ:' || l_blno || '�������󲻴���;');
        
        else
          update sccontainerplanquantity cpq
             set cpq.scpq_cnt_operator_code = nvl(l_ctnoperator_code,
                                                  cpq.scpq_cnt_operator_code),
                 
                 cpq.SCPQ_CARRIER_CONFIRM = 'Y'
           where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
        end if;
        goto nextRecord;
      else
        if (nvl(length(l_vessel_name), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_voyage), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_check_msg), 0) = 0) then
          --����������ζ���Ϊ�� ��ʼ����ƻ�
          l_check_msg := get_plan_id(l_vessel_name,
                                     l_voyage,
                                     l_org_id,
                                     l_plan_id);
        end if;
      
        if (nvl(length(l_cnt_type), 0) = 0) then
          l_check_msg := l_check_msg || '����Ϊ��;';
        end if;
      
        if (nvl(length(l_cnt_size), 0) = 0) then
          l_check_msg := l_check_msg || '�ߴ�Ϊ��;';
        end if;
        if (nvl(length(l_check_msg), 0) = 0) then
          if l_org_id in ('439','222','4392') then
            l_cnt_qnt_id := checkExist(l_plan_id, l_blno, l_eir_flag);
            if l_cnt_qnt_id = -2 then
              updateErrMsg(l_edi_id,
                           '���ڶ���ᵥ����ͬ��������:' || l_blno);
              goto nextRecord;
            end if;
          elsif l_org_id = '86' then  --TD34208
            l_cnt_qnt_id := checkExist(l_plan_id,
                                       l_blno,
                                       l_cnt_size,
                                       l_cnt_type,
                                       l_cnt_qnt);
          else
            
            l_cnt_qnt_id := checkExist(l_plan_id,
                                       l_blno,
                                       l_cnt_size,
                                       l_cnt_type,
                                       l_eir_flag);
          end if;
        else
          updateErrMsg(l_edi_id, l_check_msg);
          goto nextRecord;
        end if;
      end if;
      savepoint save1;
    
      begin
        if l_eir_flag then
          if l_record_type = 1 then -- ɾ��
            updateErrMsg(l_edi_id,'������EIR:������ɾ��ʧ�ܡ�');
          else
            updateErrMsg(l_edi_id,
                         '������EIR:' || getchange(l_cnt_qnt_id,
                                                 l_cnt_size,
                                                 l_cnt_type,
                                                 l_cnt_quentity,
                                                 l_cnt_get_place_code,
                                                 l_discharge_place_code,
                                                 l_destination_port_code,
                                                 l_remark));
          end if;
          goto nextRecord;
        end if;
        
         --У���ᵥ���ڣ����������Ƿ�һ��
        if l_paramsValue.Sapd_Value1 = '1' then
          l_check_msg := checkBLVesselVoyage(l_org_id,l_blno,l_vessel_name,l_voyage);
          if l_check_msg is not null then
            updateErrMsg(l_edi_id, l_check_msg);
            goto nextRecord;
          end if;
        end if;
      
        if l_cnt_qnt_id != -1 and l_check_bl_voyage = 'Y' and nvl(l_record_type,0) = 0 and l_org_id != '86' then
          updateErrMsg(l_edi_id, '�ú����Ѵ����ᵥ����ͬ��������;');
          goto nextRecord;
        end if;
        
        --TD34208,0:��������  1:ɾ������  2:���ǵ���   
        if l_cnt_qnt_id = -1 and nvl(l_record_type, 0) = 0 then
          insert into sccontainerplanquantity cpq
            (cpq.scpq_cnt_qnt_id,
             cpq.scpq_plan_id,
             cpq.scpq_cnt_size,
             cpq.scpq_cnt_type,
             cpq.scpq_cnt_status,
             cpq.scpq_cnt_qnt,
             cpq.scpq_bl_no,
             cpq.scpq_soc_flag,
             cpq.scpq_port_destination_code,
             cpq.scpq_remark,
             cpq.scpq_cnt_operator_code,
             cpq.scpq_creator,
             cpq.scpq_creat_time,
             cpq.scpq_cnt_delivery_place_code,
             cpq.scpq_cnt_return_place_code,
             cpq.scpq_delivery_time_start,
             cpq.scpq_delivery_time_end,
             cpq.scpq_cnt_user_name, -- ������
             cpq.scpq_discharge_port_code,
             cpq.scpq_tempeature_vent,
             cpq.scpq_data_source,
             cpq.SCPQ_CARRIER_CONFIRM,
             cpq.scpq_haulier_code)
          values
            (seq_scpq_cnt_qnt_id.nextval,
             l_plan_id,
             l_cnt_size,
             l_cnt_type,
             l_ctn_status,
             l_cnt_quentity,
             l_blno,
             nvl(l_soc,'N'),
             l_destination_port_code,
             l_remark,
             l_ctnoperator_code,
             pi_user_id,
             l_creat_time,
             l_cnt_get_place_code,
             l_cnt_return_place_code,
             l_cnt_get_validate,
             l_cnt_rtn_validate,
             l_shipper,
             l_discharge_place_code,
             l_temerature_setting,
             '1',
             decode(l_msg_type, 'XG00006501', 'Y'), --������ȷ�ϵ���
             l_haulier_code);
             update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_quentity || 'X' || l_cnt_size || l_cnt_type || ',��������ɹ�' where cl.itli_id = l_edi_id;
        elsif l_record_type = 2 then --���ǵ�����ɾ�����ٵ���
          select count(1) into l_count from temp_exp_mf where SSEM_BL_NO = l_blno;
          if l_count = 0 then
             delCtn(l_plan_id, l_blno);
             insert into temp_exp_mf (SSEM_BL_NO) values (l_blno);
          end if;
          insert into sccontainerplanquantity cpq
            (cpq.scpq_cnt_qnt_id,
             cpq.scpq_plan_id,
             cpq.scpq_cnt_size,
             cpq.scpq_cnt_type,
             cpq.scpq_cnt_status,
             cpq.scpq_cnt_qnt,
             cpq.scpq_bl_no,
             cpq.scpq_soc_flag,
             cpq.scpq_port_destination_code,
             cpq.scpq_remark,
             cpq.scpq_cnt_operator_code,
             cpq.scpq_creator,
             cpq.scpq_creat_time,
             cpq.scpq_cnt_delivery_place_code,
             cpq.scpq_cnt_return_place_code,
             cpq.scpq_delivery_time_start,
             cpq.scpq_delivery_time_end,
             cpq.scpq_cnt_user_name, -- ������
             cpq.scpq_discharge_port_code,
             cpq.scpq_tempeature_vent,
             cpq.scpq_data_source,
             cpq.SCPQ_CARRIER_CONFIRM,
             cpq.scpq_haulier_code)
          values
            (seq_scpq_cnt_qnt_id.nextval,
             l_plan_id,
             l_cnt_size,
             l_cnt_type,
             l_ctn_status,
             l_cnt_quentity,
             l_blno,
             nvl(l_soc,'N'),
             l_destination_port_code,
             l_remark,
             l_ctnoperator_code,
             pi_user_id,
             l_creat_time,
             l_cnt_get_place_code,
             l_cnt_return_place_code,
             l_cnt_get_validate,
             l_cnt_rtn_validate,
             l_shipper,
             l_discharge_place_code,
             l_temerature_setting,
             '1',
             decode(l_msg_type, 'XG00006501', 'Y'), --������ȷ�ϵ���
             l_haulier_code);
             update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_quentity || 'X' || l_cnt_size || l_cnt_type || ',���ǵ���ɹ�' where cl.itli_id = l_edi_id;              
        elsif l_cnt_qnt_id = -1 and l_record_type = 1 then     
              updateErrMsg(l_edi_id, '�ᵥ��:'|| l_blno || ',���ͳߴ�:' || l_cnt_size || l_cnt_type || ',�޴�������,�޷�ɾ��!;');
        elsif l_cnt_qnt_id > 0 and  l_record_type = 1 then
              delete from sccontainerplanquantity cpq where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
              update ictnlistin cl set cl.itli_err_desc = '�ᵥ��:' || l_blno || ',���ͳߴ�:' || l_cnt_qnt || 'X' || l_cnt_size || l_cnt_type || ',ɾ������ɹ�:' where cl.itli_id = l_edi_id;
        else
          l_change := getchange(l_cnt_qnt_id,
                                l_cnt_size,
                                l_cnt_type,
                                l_cnt_quentity,
                                l_cnt_get_place_code,
                                l_discharge_place_code,
                                l_destination_port_code,
                                l_remark);
          update ictnlistin cl
             set cl.itli_err_desc = '���µ���ɹ�:' || l_change
           where cl.itli_id = l_edi_id;
        
          update sccontainerplanquantity cpq
             set cpq.scpq_plan_id                 = nvl(l_plan_id,
                                                        cpq.scpq_plan_id),
                 cpq.scpq_cnt_size                = nvl(l_cnt_size,
                                                        cpq.scpq_cnt_size),
                 cpq.scpq_cnt_type                = nvl(l_cnt_type,
                                                        cpq.scpq_cnt_type),
                 cpq.scpq_cnt_status              = nvl(l_ctn_status,
                                                        cpq.scpq_cnt_status),
                 cpq.scpq_cnt_qnt                 = nvl(l_cnt_quentity,
                                                        cpq.scpq_cnt_qnt),
                 cpq.scpq_bl_no                   = nvl(l_blno,
                                                        cpq.scpq_bl_no),
                 cpq.scpq_soc_flag                = nvl(l_soc,
                                                        cpq.scpq_soc_flag),
                 cpq.scpq_port_destination_code   = nvl(l_destination_port_code,
                                                        cpq.scpq_port_destination_code),
                 cpq.scpq_remark                  = nvl(l_remark,
                                                        cpq.scpq_remark),
                 cpq.scpq_cnt_operator_code       = nvl(l_ctnoperator_code,
                                                        cpq.scpq_cnt_operator_code),
                 cpq.scpq_cnt_delivery_place_code = nvl(l_cnt_get_place_code,
                                                        cpq.scpq_cnt_delivery_place_code),
                 cpq.scpq_cnt_return_place_code   = nvl(l_cnt_return_place_code,
                                                        cpq.scpq_cnt_return_place_code),
                 cpq.scpq_delivery_time_start     = nvl(l_cnt_get_validate,
                                                        cpq.scpq_delivery_time_start),
                 cpq.scpq_delivery_time_end       = nvl(l_cnt_rtn_validate,
                                                        cpq.scpq_delivery_time_end),
                 cpq.scpq_cnt_user_name           = nvl(l_shipper,
                                                        cpq.scpq_cnt_user_name), -- ������
                 cpq.scpq_discharge_port_code     = nvl(l_discharge_place_code,
                                                        cpq.scpq_discharge_port_code),
                 cpq.scpq_tempeature_vent         = nvl(l_temerature_setting,
                                                        cpq.scpq_tempeature_vent),
                 cpq.scpq_data_source             = '1',
                 cpq.scpq_haulier_code            = nvl(l_haulier_code,
                                                        cpq.scpq_haulier_code)
           where cpq.scpq_cnt_qnt_id = l_cnt_qnt_id;
        end if;
        update ictnlistin cl
           set cl.itli_disposeflag = 'Y'
         where cl.itli_id = l_edi_id;
      exception
        when others then
          rollback to save1;
          updateErrMsg(l_edi_id, SQLERRM);
      end;
      --commit; --HY3-838
    end loop;
    close cur_ctn_list;--HY3-838
  end import;

  /************************************
  *
  * �������Զ�����  
  * ���� ����� 2007-11-21
  * ���� @pi_batch_no �������κ�
  *      @po_result ������
  *
  *************************************/

  procedure auto_import(pi_batch_no varchar2, -- ���κ�
                        po_result   out varchar2) -- ������
   is
    l_userId cuser.cusr_user_id%type;
    v_org_id ictnlistin.itli_orgid%type;
  begin
    select i.itli_orgid
      into v_org_id
      from ictnlistin i
     where i.itli_batchno = pi_batch_no
       and i.itli_orgid is not null
       and rownum = 1;
       
    select u.cusr_user_id
      into l_userId
      from cuser u
     where u.cusr_user_id = '5';
    import(pi_batch_no, l_userId);
  exception
    when others then
      po_result := sqlerrm;
  end auto_import;
end edi_ctn_requirement_in_pkg;
/
