create or replace package EDI_EXP_MANIFEST_VERIFY_IN is

  -- Author  : ADMINISTRATOR
  -- Created : 2018/4/19 14:03:19
  -- Purpose : ��������ǩ���˶Ե���
  
  type rec_col is record(
    colname   varchar2(30),
    colvalue1 varchar2(2000),
    colvalue2 varchar2(2000),
    seqno     varchar2(3),
    cntno     varchar2(15),
    tbltype   char(1),
    resultCode varchar2(1),
    resultDesc varchar2(2000));
  type t_col is table of rec_col index by binary_integer;
  
  -- Public type declarations
  procedure importVerify(pi_batch_no  in varchar2,
                         pi_org_id    in varchar2,
                         pi_user_id   in varchar2,
                         po_result    out sys_refcursor);
                         
                         
  procedure verifyExpManifest(pi_batch_no  in varchar2,
                              pi_org_id    in varchar2,
                              po_result    out sys_refcursor);

end EDI_EXP_MANIFEST_VERIFY_IN;
/
create or replace package body EDI_EXP_MANIFEST_VERIFY_IN is

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

  procedure updateImfiDipose(pi_imfi_id  in varchar2,
                             pi_dispose  in varchar2,
                             pi_message  in varchar2) as
  begin
    update imanifestin imfi
       set imfi.imfi_err_reason   = substrb(pi_message,1,600),
           imfi.imfi_dispose_flag = pi_dispose,
           imfi.imfi_dispose_time = sysdate
     where imfi.imfi_id = pi_imfi_id;
  end updateImfiDipose;

  --��ȡ������Ϣ
  function getVoyageId(pi_org_id  in varchar2,
                       pi_imfRow  in imanifestin%rowtype)
    return varchar2 as
    l_voyage_id sexportmanifest.ssem_voyage_id%type;
    pi_schedule_check_type  cbizconfig.cbic_sc_value%type;
  begin
     pi_schedule_check_type := edi_util_pkg.get_bizconfig('SAHU_SCHEDULE_CHECK_RULE',
                                                          pi_org_id,'1','1');
     if pi_schedule_check_type = '1' then
       l_voyage_id := edi_util_pkg.getExpVoyageId(pi_imfRow.Imfi_Vessel_Code,
                                                  pi_imfRow.Imfi_Vessel_Name,
                                                  pi_imfRow.Imfi_Voyage,
                                                  pi_org_id);
      
     else
       l_voyage_id := edi_util_pkg.getJSExpVoyageId(pi_imfRow.Imfi_Vessel_Name,
                                                    pi_imfRow.Imfi_Voyage,
                                                    pi_imfRow.Imfi_Load_Port_Code,
                                                    pi_org_id);
     end if;
    return l_voyage_id;
  end getVoyageId;
  
  /**
  ���ܣ������ᵥ�š�����ID��ȡ�ᵥҵ������ID
  ������pi_bl_no �ᵥ��
        pi_voyage_id ����ID
        PI_ORG_ID ��˾ID
  **/
  function getManifestId(pi_bl_no     in varchar2,
                         pi_voyage_id in varchar2,
                         pi_org_id    in varchar2) return varchar2 as
    l_exp_bl_id sexportmanifest.ssem_exp_bl_id%type;
  begin
  
    begin
      select ssem.ssem_exp_bl_id
        into l_exp_bl_id
        from sexportmanifest ssem
       where ssem.ssem_bl_no = pi_bl_no
         and ssem.ssem_voyage_id = pi_voyage_id
         and ssem.ssem_org_id = pi_org_id
         and ssem.ssem_document_type = '0';
    exception
      when no_data_found then
        l_exp_bl_id := -1;
      when too_many_rows then
        l_exp_bl_id := -2;
    end;
  
    return l_exp_bl_id;
  
  end getManifestId;
  
  --�����ᵥ ǩ��״̬��ǩ����ʽ
  procedure handleManifest(pi_exp_id       in varchar2,
                           pi_user_id      in varchar2,
                           pi_doc_status   in varchar2,
                           pi_release_type in varchar2,
                           pi_memo         in varchar2)is
  begin
    update sexportmanifest sef
       set sef.ssem_release_bl_type = nvl(pi_release_type,sef.ssem_release_bl_type),
           sef.ssem_document_status = nvl(pi_doc_status,sef.ssem_document_status),
           sef.ssem_release_bl_memo = pi_memo,
           sef.ssem_last_modifier = pi_user_id,
           sef.ssem_modify_time = sysdate
     where sef.ssem_exp_bl_id = pi_exp_id; 
  end handleManifest;
  
  /**
  ***�˶�����Ϣ
  **/
  procedure verifyCntInfo(pi_imfi_id   in varchar2,
                          pi_exp_id    in varchar2,
                          po_message   out varchar2)is
    l_cntRow     imanifestincontainer%rowtype;
    l_cnt_id     scontainerinfo.spci_packing_list_id%type;
    l_cnt_no     scontainerinfo.spci_cnt_no%type;
    l_seal_no    scontainerinfo.spci_seal_no%type;
    l_seal_no2   scontainerinfo.spci_seal_no2%type;
    l_seal_no3   scontainerinfo.spci_seal_no3%type;
    l_quantity   scontainerinfo.spci_quantity%type;
    l_weight     scontainerinfo.spci_weight%type;
    
    l_count_seal int:= 0;
    l_i_seal_count int := 0;
    l_seal_count   int := 0;
    
    l_ctn_nos      clob;
    cursor cur_icnt is
      select imti.imti_id,imti.imti_container_no,
             imti.imti_ctn_package_number,imti.imti_cargo_net_weight
        from imanifestincontainer imti
       where imti.imti_pid = pi_imfi_id;
  begin
    select f_link_lob(s.spci_cnt_no)
      into l_ctn_nos
      from scontainerinfo s 
     where s.spci_exp_bl_id = pi_exp_id
       and not exists(select 0 from imanifestincontainer i 
                       where i.imti_pid = pi_imfi_id and s.spci_cnt_no = i.imti_container_no);
    
    if l_ctn_nos is not null then
      po_message := l_ctn_nos || 'Ǧ��Ų�һ��';
      return;
    end if;
    open cur_icnt;
    loop
      fetch cur_icnt
       into l_cntRow.Imti_Id,l_cntRow.Imti_Container_No,
            l_cntRow.Imti_Ctn_Package_Number,l_cntRow.Imti_Cargo_Net_Weight;
       exit when cur_icnt%notfound;
       l_count_seal   := 0;
       l_i_seal_count := 0;
       l_seal_count   := 0;
       begin
         select s.spci_packing_list_id ,s.spci_cnt_no,s.spci_seal_no,s.spci_quantity,s.spci_weight,
                s.spci_seal_no2,s.spci_seal_no3
           into l_cnt_id,l_cnt_no,l_seal_no,l_quantity,l_weight,l_seal_no2,l_seal_no3
           from scontainerinfo s
          where s.spci_exp_bl_id = pi_exp_id
            and s.spci_cnt_no = l_cntRow.Imti_Container_No
            and rownum = 1;
       exception
         when no_data_found then
           null;
       end;
       if l_cnt_id is null then -- ����ţ������ᵥ����ע��Ϣ
         po_message := l_cntRow.Imti_Container_No||'����ϵͳ��';
       else
         if l_seal_no is not null then
           l_seal_count := l_seal_count + 1;
         end if; 
         if l_seal_no2 is not null then
           l_seal_count := l_seal_count + 1;
         end if;
         if l_seal_no3 is not null then
           l_seal_count := l_seal_count + 1;
         end if;
         select count(0)
           into l_i_seal_count
           from imanifestinseal i
          where i.imsi_pid = l_cntRow.Imti_Id;
         if l_i_seal_count > 0 and l_seal_count = l_i_seal_count  then --HY3-816
           select count(0)
             into l_count_seal
             from imanifestinseal i
            where i.imsi_pid = l_cntRow.Imti_Id
              and (i.imsi_seal_no = l_seal_no or i.imsi_seal_no = l_seal_no2
                     or i.imsi_seal_no = l_seal_no3);
         elsif l_i_seal_count = 0 and l_seal_count > 0 then
             l_i_seal_count := 1;  
         end if;
         if l_i_seal_count <> l_count_seal or l_quantity <> l_cntRow.Imti_Ctn_Package_Number
            or l_cntRow.Imti_Cargo_Net_Weight <> l_weight then
           po_message := substrb(l_cntRow.Imti_Container_No ||'Ǧ���/����/ë�ز�һ��;'|| po_message,1,1000);
         end if;
       end if;
    end loop;
    close cur_icnt;
  end verifyCntInfo;
  
  --�˶�Ʒ��
  procedure verifyCargoInfo(pi_imfi_id   in varchar2,
                            pi_exp_id    in varchar2,
                            po_message   in out varchar2)is
    l_cargo_verify_count  int:= 0;
  begin
     select count(0)
       into l_cargo_verify_count
       from imanifestincargo imai,scargoinfo spgi
      where imai.imai_pid = pi_imfi_id
        and spgi.spgi_exp_bl_id = pi_exp_id
        and spgi.spgi_record_type = '0'
        and (SYS.UTL_MATCH.edit_distance_similarity(substr(upper(spgi.spgi_cargo_description_en),1,10),
                                                    substr(upper(imai.imai_cargo_description),1,10)) >= 80
             or instr(replace(replace(upper(spgi.spgi_cargo_description_en),chr(10),' '),' ',''),
                      substr(replace(replace(upper(imai.imai_cargo_description),chr(10),' '),' ',''),1,10)) > 0);
        
    if l_cargo_verify_count = 0 then
      po_message := substrb('*;'|| po_message,1,1000);
    end if;
    
  end verifyCargoInfo;

  /**
   ** ɽ���ᵥ�˶�
   */
  procedure verifyInfoSD(pi_imfi_id   in varchar2,
                          pi_exp_id    in varchar2,
                          po_message   out varchar2,
                          data_code in varchar2,
                          pi_bl_no in varchar2,
                          pi_org_id in varchar2,
                          pi_user_id in varchar2)is
    l_cntRow     imanifestincontainer%rowtype;
    l_cnt_id     scontainerinfo.spci_packing_list_id%type;
    l_cnt_no     scontainerinfo.spci_cnt_no%type;
    l_seal_no    scontainerinfo.spci_seal_no%type;
    l_seal_no2   scontainerinfo.spci_seal_no2%type;
    l_seal_no3   scontainerinfo.spci_seal_no3%type;
    l_quantity   scontainerinfo.spci_quantity%type;
    l_weight     scontainerinfo.spci_weight%type;
    f_seal_no    scontainerinfo.spci_seal_no%type;
    l_measurement    scontainerinfo.spci_measurement%type;
    f_i_seal_no    imanifestinseal.imsi_seal_no%type;
    l_i_description    imanifestincargo.imai_cargo_description%type;
    l_description    scargoinfo.spgi_cargo_description_en%type;

    l_count_seal int:= 0;
    l_i_seal_count int := 0;
    l_seal_count   int := 0;
    l_cargo_verify_count  int:= 0;

  if data_code is not null then
    l_ctn_nos      clob;
    cursor cur_icnt is
      select imti.imti_id,imti.imti_container_no,
           imti.imti_ctn_package_number,imti.imti_cargo_net_weight,imti.imti_cargo_measurement
      from imanifestincontainer imti
      where imti.imti_pid = pi_imfi_id;
    begin
      select f_link_lob(s.spci_cnt_no)
        into l_ctn_nos
      from scontainerinfo s
      where s.spci_exp_bl_id = pi_exp_id
        and not exists(select 0 from imanifestincontainer i
                     where i.imti_pid = pi_imfi_id and s.spci_cnt_no = i.imti_container_no);

    if l_ctn_nos is not null then
          po_message := l_ctn_nos || '��Ų�һ��';
    return;
    end if;

    if (INSTR(data_code,"SPGI_CARGO_DESCRIPTION_EN") > 0) then
        select count(0),imai.imai_cargo_description,spgi.spgi_cargo_description_en
            into l_cargo_verify_count,l_i_description,l_description
        from imanifestincargo imai,scargoinfo spgi
            where imai.imai_pid = pi_imfi_id
             and spgi.spgi_exp_bl_id = pi_exp_id
             and spgi.spgi_record_type = '0'
             and (SYS.UTL_MATCH.edit_distance_similarity(substr(upper(spgi.spgi_cargo_description_en),1,10),
                                                      substr(upper(imai.imai_cargo_description),1,10)) >= 80
             or instr(replace(replace(upper(spgi.spgi_cargo_description_en),chr(10),' '),' ',''),
                     substr(replace(replace(upper(imai.imai_cargo_description),chr(10),' '),' ',''),1,10)) > 0);

        if l_cargo_verify_count = 0 then
            select imai.imai_cargo_description
              into l_i_description
              from imanifestincargo imai
            where imai.imai_pid = pi_imfi_id
            select spgi.spgi_cargo_description_en
              into l_description
              from scargoinfo spgi
            where spgi.spgi_exp_bl_id = pi_exp_id
              and spgi.spgi_record_type = '0';
            insert into sbcheckresult(sbcr_id,
                                      sbcr_bl_id,
                                      sbcr_imfi_id,
                                      sbcr_bl_no,
                                      sbcr_cnt_no,
                                      sbcr_data,
                                      sbcr_mf_value,
                                      sbcr_bl_value,
                                      sbcr_check_flag,
                                      sbcr_org_id,
                                      sbcr_creator,
                                      sbcr_create_time)
            values (seq_sbcr_id.nextval,
                    pi_exp_id,
                    pi_imfi_id,
                    pi_bl_no,
                    l_cnt_no,
                    "Ʒ��",
                    l_description,
                    l_i_description,
                    "Y",
                    pi_org_id,
                    pi_user_id,
                    sysdate);
        else
            insert into sbcheckresult(sbcr_id,
                                      sbcr_bl_id,
                                      sbcr_imfi_id,
                                      sbcr_bl_no,
                                      sbcr_cnt_no,
                                      sbcr_data,
                                      sbcr_mf_value,
                                      sbcr_bl_value,
                                      sbcr_check_flag,
                                      sbcr_org_id,
                                      sbcr_creator,
                                      sbcr_create_time)
            values (seq_sbcr_id.nextval,
                    pi_exp_id,
                    pi_imfi_id,
                    pi_bl_no,
                    l_cnt_no,
                    "Ʒ��",
                    l_description,
                    l_i_description,
                    "N",
                    pi_org_id,
                    pi_user_id,
                    sysdate);

        end if;
    end if;

    open cur_icnt;
        loop
            fetch cur_icnt
             into l_cntRow.Imti_Id,l_cntRow.Imti_Container_No,
                 l_cntRow.Imti_Ctn_Package_Number,l_cntRow.Imti_Cargo_Net_Weight,l_cntRow.Imti_Cargo_Measurement;
        exit when cur_icnt%notfound;
        l_count_seal   := 0;
        l_i_seal_count := 0;
        l_seal_count   := 0;
    begin
        select s.spci_packing_list_id ,s.spci_cnt_no,s.spci_seal_no,s.spci_quantity,s.spci_weight,
            s.spci_seal_no2,s.spci_seal_no3,s.spci_measurement
        into l_cnt_id,l_cnt_no,l_seal_no,l_quantity,l_weight,l_seal_no2,l_seal_no3,l_measurement
        from scontainerinfo s
        where s.spci_exp_bl_id = pi_exp_id
          and s.spci_cnt_no = l_cntRow.Imti_Container_No
          and rownum = 1;
        exception
           when no_data_found then
             null;
           end;
        if l_cnt_id is null then -- ����ţ������ᵥ����ע��Ϣ
             po_message := l_cntRow.Imti_Container_No||'����ϵͳ��';
        else
            if l_seal_no is not null then
                l_seal_count := l_seal_count + 1
                f_seal_no = l_seal_no;
        end if;

        if l_seal_no2 is not null then
           l_seal_count := l_seal_count + 1;
            f_seal_no = l_seal_no2;
        end if;
        if l_seal_no3 is not null then
           l_seal_count := l_seal_count + 1;
            f_seal_no = l_seal_no3;
        end if;

        select count(0)
          into l_i_seal_count
          from imanifestinseal i
          where i.imsi_pid = l_cntRow.Imti_Id;

        if l_i_seal_count > 0 and l_seal_count = l_i_seal_count  then --HY3-816
            select count(0),i.imsi_seal_no
            into l_count_seal,f_i_seal_no
            from imanifestinseal i
            where i.imsi_pid = l_cntRow.Imti_Id
              and (i.imsi_seal_no = l_seal_no or i.imsi_seal_no = l_seal_no2
              or i.imsi_seal_no = l_seal_no3);
        elsif l_i_seal_count = 0 and l_seal_count > 0 then
             l_i_seal_count := 1;
        end if;

        if l_i_seal_count <> l_count_seal or l_quantity <> l_cntRow.Imti_Ctn_Package_Number
             or l_cntRow.Imti_Cargo_Net_Weight <> l_weight then
             po_message := substrb(l_cntRow.Imti_Container_No ||'Ǧ���/����/ë�ز�һ��;'|| po_message,1,1000);
        end if;

        if(INSTR(data_code,"SPCI_SEAL_NO") > 0) then
            if l_i_seal_count <> l_count_seal then
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "���",
                        f_seal_no,
                        f_i_seal_no,
                        "Y",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
                else
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "���",
                        f_seal_no,
                        f_i_seal_no,
                        "N",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
            end if;
        end if;

        if(INSTR(data_code,"SPCI_MEASUREMENT") > 0) then
            if l_cntRow.Imti_Cargo_Measurement <> l_measurement then
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "���",
                        l_measurement,
                        l_cntRow.Imti_Cargo_Measurement,
                        "Y",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
                else
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "���",
                        l_measurement,
                        l_cntRow.Imti_Cargo_Measurement,
                        "N",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
            end if;
        end if;

        if(INSTR(data_code,"SPCI_WEIGHT") > 0) then
            if l_cntRow.Imti_Cargo_Net_Weight <> l_weight then
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "����",
                        l_weight,
                        l_cntRow.Imti_Cargo_Net_Weight,
                        "Y",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
                else
                insert into sbcheckresult(sbcr_id,
                                          sbcr_bl_id,
                                          sbcr_imfi_id,
                                          sbcr_bl_no,
                                          sbcr_cnt_no,
                                          sbcr_data,
                                          sbcr_mf_value,
                                          sbcr_bl_value,
                                          sbcr_check_flag,
                                          sbcr_org_id,
                                          sbcr_creator,
                                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "����",
                        l_weight,
                        l_cntRow.Imti_Cargo_Net_Weight,
                        "N",
                        pi_org_id,
                        pi_user_id,
                        sysdate);

            end if;
        end if;

        if (INSTR(data_code,"SPCI_QUANTITY") > 0) then
          if l_quantity <> l_cntRow.Imti_Ctn_Package_Number then
            insert into sbcheckresult(sbcr_id,
                                      sbcr_bl_id,
                                      sbcr_imfi_id,
                                      sbcr_bl_no,
                                      sbcr_cnt_no,
                                      sbcr_data,
                                      sbcr_mf_value,
                                      sbcr_bl_value,
                                      sbcr_check_flag,
                                      sbcr_org_id,
                                      sbcr_creator,
                                      sbcr_create_time)
                values (seq_sbcr_id.nextval,
                        pi_exp_id,
                        pi_imfi_id,
                        pi_bl_no,
                        l_cnt_no,
                        "����",
                        l_quantity,
                        l_cntRow.Imti_Ctn_Package_Number,
                        "Y",
                        pi_org_id,
                        pi_user_id,
                        sysdate);
            else
                insert into sbcheckresult(sbcr_id,
                          sbcr_bl_id,
                          sbcr_imfi_id,
                          sbcr_bl_no,
                          sbcr_cnt_no,
                          sbcr_data,
                          sbcr_mf_value,
                          sbcr_bl_value,
                          sbcr_check_flag,
                          sbcr_org_id,
                          sbcr_creator,
                          sbcr_create_time)
                values (seq_sbcr_id.nextval,
                          pi_exp_id,
                          pi_imfi_id,
                          pi_bl_no,
                          l_cnt_no,
                          "����",
                          l_quantity,
                          l_cntRow.Imti_Ctn_Package_Number,
                          "N",
                          pi_org_id,
                          pi_user_id,
                          sysdate);
            end if;
        end if;

        end if;
        end loop;

        close cur_icnt;

    end if;
  end verifyInfoSD;

  /*
  ** ��������ǩ���˶Ե���
  **/
  procedure importVerify(pi_batch_no  in varchar2,
                         pi_org_id    in varchar2,
                         pi_user_id   in varchar2,
                         po_result    out sys_refcursor)is
    l_imfRow     imanifestin%rowtype;
                         
    l_voyage_id  sexportmanifest.ssem_voyage_id%type;
    l_exp_bl_id  sexportmanifest.ssem_exp_bl_id%type;
    l_doc_status sexportmanifest.ssem_document_status%type;
    l_release_type sexportmanifest.ssem_release_bl_type%type;
    l_release_memo sexportmanifest.ssem_release_bl_memo%type;
    l_carrier_id sexportmanifest.ssem_carrier_id%type;

    l_upd_release_type imanifestin.imfi_bl_send_mode_code%type;
    l_upd_doc_status   sexportmanifest.ssem_document_status%type;
    l_upd_release_memo sexportmanifest.ssem_release_bl_memo%type;
    data_code sblcheckconfig.sbcc_data_code%type;
    l_bl_no sexportmanifest.ssem_bl_no;

    l_message    varchar2(1000);
    l_vessel_count int := 0;
    
    cursor cur_imfi is
      select imf.imfi_id,imf.imfi_bl_no,imf.imfi_vessel_code,
             imf.imfi_vessel_name,imf.imfi_voyage,imf.imfi_load_port_code,
             imf.imfi_bl_issue_mode_code
        from imanifestin imf
       where imf.imfi_batch_no = pi_batch_no
         and imf.imfi_dispose_flag = 'N';
  begin
    open cur_imfi;
    loop
      <<nextRecord>>
      fetch cur_imfi
       into l_imfRow.Imfi_Id,l_imfRow.Imfi_Bl_No,l_imfRow.Imfi_Vessel_Code,
            l_imfRow.Imfi_Vessel_Name,l_imfRow.Imfi_Voyage,l_imfRow.Imfi_Load_Port_Code,
            l_imfRow.Imfi_Bl_Issue_Mode_Code;
      exit when cur_imfi%notfound;
      l_voyage_id := null;
      l_exp_bl_id := null;
      l_message   := null;
      l_upd_release_type := null;
      l_upd_doc_status   := null;
      l_upd_release_memo := null;
      l_vessel_count := 0;
      if l_imfRow.Imfi_Vessel_Code is not null then
        select count(0)
          into l_vessel_count
          from cshipcanonical c
         where c.cshc_id = l_imfRow.Imfi_Vessel_Code;
         if l_vessel_count = 0 then
           goto nextRecord;
         end if;
      elsif l_imfRow.Imfi_Vessel_Name is not null then
        select count(0)
          into l_vessel_count
          from cshipcanonical c
         where c.cshc_en_vessel = l_imfRow.Imfi_Vessel_Name
           and c.cshc_org_id = pi_org_id
           and c.cshc_cancel_flag = 'N';
         if l_vessel_count = 0 then
           goto nextRecord;
         end if;
      end if;
      begin
        l_voyage_id := getVoyageId(pi_org_id,l_imfRow);
      exception
        when others then
          l_voyage_id := null;
      end;
      if l_voyage_id is null then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ�ﴬ�����ڣ����Ǻ��β�һ��');
        goto nextRecord;
      end if;
      l_exp_bl_id := getManifestId(l_imfRow.Imfi_Bl_No,l_voyage_id,pi_org_id);
      if l_exp_bl_id = -1 or l_exp_bl_id = -2 then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ�޷�ƥ���ᵥ���ᵥ�ظ�');
        goto nextRecord;
      end if;
      /* δ�ŵ�         ����      0  
                        ���      1     ����������������ᵥ��δ��ˣ�
                        �ᵥȷ��  2     ��ȷ�������������ᵥ������ˣ�δ�ᵥȷ�ϣ�
                        �ŵ�      3     ���ŵ������������ᵥ����ȷ�ϣ�δ�ŵ���
         ����ŵ�       ���ŵ�    4     �����ŵ�ԭ��д�ڷŵ���ע�У� 
         �ɷŵ�                   5 */
      select sef.ssem_document_status,
             sef.ssem_release_bl_type,
             sef.ssem_release_bl_memo,
             sef.ssem_carrier_id,
             sef.ssem_bl_no
        into l_doc_status ,l_release_type,l_release_memo,l_carrier_id,l_bl_no
        from sexportmanifest sef
       where sef.ssem_exp_bl_id = l_exp_bl_id;

      select sbcc_data_code into data_code
        from sblcheckconfig sbc
        where sbc.sbcc_carrier = l_carrier_id
        and sbcc_cancel_flag = 'Y'
        and sbcc_org_id = pi_org_id;

      if l_doc_status = 0 or l_doc_status = '5' then --0δ�ŵ� 5�ɷŵ�
        if pi_org_id = '139' then
        verifyInfoSD(l_imfRow.Imfi_Id,l_exp_bl_id,l_message,data_code,l_bl_no,pi_org_id,pi_user_id);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        else
        verifyCntInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        end if;

        if l_message is null then
          l_upd_doc_status := 5;--�˶��޲��죬���Ϊ�ɷŵ�
          if l_release_memo is null then
            l_message := '�˶��޲��죬�ɷŵ�';
            updateImfiDipose(l_imfRow.Imfi_Id,'Y',l_message);
          else
            l_upd_release_memo := '�ᵥ����һ�£��ɷŵ�';
            updateImfiDipose(l_imfRow.Imfi_Id,'Y','�ᵥ����һ�£��ɷŵ�');
          end if;
        else
          l_upd_doc_status := 4;--�˶��в��죬���Ϊ�����ᵥ
          l_upd_release_memo := substrb(l_message,1,600);
          updateImfiDipose(l_imfRow.Imfi_Id,'D',substrb(l_message,1,600));
        end if;
        if l_doc_status = '5' and l_message is not null then
          edi_util_pkg.edi_register_event('�ɷŵ��ᵥ�˶Բ���:'|| l_message,'SSOA_EXP_MAIFEST_VERIFY',pi_org_id,l_exp_bl_id,'E',null,null,null);
        end if;
        
        if l_imfRow.Imfi_Bl_Issue_Mode_Code in (1,3,5) then
          l_upd_release_type := l_imfRow.Imfi_Bl_Issue_Mode_Code;
        else
          l_message := l_message||',ǩ����ʽ�쳣;'; 
          l_upd_release_memo := l_upd_release_memo ||',ǩ����ʽ�쳣;'; 
          updateImfiDipose(l_imfRow.Imfi_Id,'D',l_message);
        end if;
        handleManifest(l_exp_bl_id,pi_user_id,l_upd_doc_status,l_upd_release_type,l_upd_release_memo);
      elsif l_doc_status = 4 then --�����ᵥ
        if pi_org_id = '139' then
        verifyInfoSD(l_imfRow.Imfi_Id,l_exp_bl_id,l_message,data_code,l_bl_no,pi_org_id,pi_user_id);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        else
        verifyCntInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        end if;
        if l_message is null then
          l_upd_doc_status := 4;--�˶��޲��죬���Ϊ�ɷŵ� HY3-1804 ������ᵥ�������ᵥ����ʹMSC���º������ϵͳ�ȶ���ȷ��Ҳ������״̬Ϊ�ɷŵ���
          l_message := '�˶��޲��죬�ɷŵ�';
          updateImfiDipose(l_imfRow.Imfi_Id,'Y',l_message);
          if l_release_memo is not null then
            l_upd_release_memo := '�ᵥ����һ��,�ɷŵ�';
          end if;
          edi_util_pkg.edi_register_event('�����ᵥ�˶Բ���:'|| l_message,'SSOA_EXP_MAIFEST_VERIFY',pi_org_id,l_exp_bl_id,'E',null,null,null);
        else
          l_upd_doc_status := 4;--�˶��в��죬���Ϊ�����ᵥ
          l_upd_release_memo := substrb(l_message,1,600);
          updateImfiDipose(l_imfRow.Imfi_Id,'D',substrb(l_message,1,600));
        end if;
        
        if l_imfRow.Imfi_Bl_Issue_Mode_Code in (1,3,5) then
          l_upd_release_type := l_imfRow.Imfi_Bl_Issue_Mode_Code;
        else
          l_message := l_message||',ǩ����ʽ�쳣;';
          l_upd_release_memo := l_upd_release_memo ||',ǩ����ʽ�쳣;';  
          updateImfiDipose(l_imfRow.Imfi_Id,'D',l_message);
        end if;
        
        if l_imfRow.Imfi_Bl_Send_Mode_Code <> l_release_type then --�˶�ǩ����ʽ
           l_message := l_message||',ǩ����ʽ�˶Բ�һ��'; 
           updateImfiDipose(l_imfRow.Imfi_Id,'D',l_message); 
        end if;
        handleManifest(l_exp_bl_id,pi_user_id,l_upd_doc_status,l_upd_release_type,l_upd_release_memo);
      elsif l_doc_status = 3 then --�ѷŵ�
        if pi_org_id = '139' then
        verifyInfoSD(l_imfRow.Imfi_Id,l_exp_bl_id,l_message,data_code,l_bl_no,pi_org_id,pi_user_id);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        else
        verifyCntInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        verifyCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
        end if;
        if l_message is null and (l_imfRow.Imfi_Bl_Send_Mode_Code = l_release_type
            or l_imfRow.Imfi_Bl_Send_Mode_Code is null) then
          l_message := '�ѷŵ��ᵥ�˶��޲���';
          updateImfiDipose(l_imfRow.Imfi_Id,'Y',l_message);
        else
          if l_imfRow.Imfi_Bl_Send_Mode_Code <> l_release_type then
            l_message := 'ǩ����ʽ��һ��,'|| l_message ; 
          end if;
          edi_util_pkg.edi_register_event('�ѷŵ��ᵥ�˶Բ���:'|| l_message,'SSOA_EXP_MAIFEST_VERIFY',pi_org_id,l_exp_bl_id,'E',null,null,null);
          updateImfiDipose(l_imfRow.Imfi_Id,'D','�ѷŵ��ᵥ�˶Բ���:'||l_message);
        end if;
      end if;
    end loop;
    close cur_imfi;
    commit;
    
    open po_result for
      select imf.imfi_bl_no BL_NO,
             imf.imfi_err_reason RESULT,
             imf.imfi_dispose_flag RESULT_TYPE
        from imanifestin imf
       where imf.imfi_batch_no = pi_batch_no;
  end importVerify;
  
  --�˶Ի�����Ϣ
  procedure verifyTotalCargoInfo(pi_imfi_id   in varchar2,
                                 pi_exp_id    in varchar2,
                                 po_message   out varchar2)is
    v_i_gross_weight  imanifestincargo.imai_cargo_gross_weight%type;
    v_i_quantity      imanifestincargo.imai_cargo_package_number%type;
    v_i_measurement   imanifestincargo.imai_cargo_measurement%type;
    v_i_cargo_count   int := 0;
    
    v_gross_weight    scargoinfo.spgi_gross_weight%type;
    v_quantity        scargoinfo.spgi_quantity%type;
    v_measurement     scargoinfo.spgi_measurement%type;
    v_cargo_count     int := 0;

  begin
    select nvl(sum(i.imai_cargo_gross_weight),0),
           nvl(sum(i.imai_cargo_package_number),0),
           nvl(trunc(sum(i.imai_cargo_measurement),3),0),
           count(0)
      into v_i_gross_weight,v_i_quantity,v_i_measurement,v_i_cargo_count
      from imanifestincargo i 
     where i.imai_pid = pi_imfi_id;
     
    select nvl(s.spgi_gross_weight,0),nvl(s.spgi_quantity,0),nvl(trunc(s.spgi_measurement,3),0)
      into v_gross_weight,v_quantity,v_measurement
      from scargoinfo s 
     where s.spgi_exp_bl_id =  pi_exp_id
       and s.spgi_record_type = 1
       and rownum = 1;
    
    select count(0) into v_cargo_count from scargoinfo s where s.spgi_exp_bl_id = pi_exp_id and s.spgi_record_type = '0';
       
    if v_i_gross_weight <> v_gross_weight or 
      v_i_quantity <> v_quantity or v_i_measurement <> v_measurement then
      po_message := '�������/����/�ߴ� �в���';
    end if;
    
/*    if v_cargo_count <> v_i_cargo_count then
      po_message := po_message || ';����Ʒ�� �в���';
    else
      v_i_cargo_count := 0;
      select count(0)
        into v_i_cargo_count
        from scargoinfo s
       where s.spgi_exp_bl_id = pi_exp_id
         and s.spgi_record_type = '0'
         and exists(select 0 from imanifestincargo i 
               where i.imai_pid = pi_imfi_id 
                 and i.imai_cargo_description = s.spgi_cargo_description_en);
      if v_cargo_count <> v_i_cargo_count then
        po_message := po_message || ';����Ʒ�� �в���';
      end if;
    end if;*/    
    
  end verifyTotalCargoInfo;
  
  procedure getCompareCol(pi_exp_bl_id   in varchar2,
                          pi_imfi_id     in varchar2,
                          pi_compare_col out t_col)  as
    i int := 0;
    begin
    for l_record in (select 'SSEM_CARRIER_ID' as colname,
                             s.ssem_carrier_id as colvalue1,
                             i.imfi_carrier_code as colvalue2,
                             null as seqno,
                             null as cntno,
                             '0' as tbltype
                        from sexportmanifest s, imanifestin i
                       where s.ssem_exp_bl_id = pi_exp_bl_id
                         and i.imfi_id = pi_imfi_id
                         and s.ssem_carrier_id <> i.imfi_carrier_code
                      union
                      select 'SHSS_EMP_VOYAGE_CODE' as colname,
                             sh.shss_exp_voyage_code as colvalue1,
                             i.imfi_voyage as colvalue2,
                             null as seqno,
                             null as cntno,
                             '0' as tbltype
                        from sexportmanifest s, shsailingschedule sh, imanifestin i
                       where s.ssem_voyage_id = sh.shss_voyage_id
                         and s.ssem_exp_bl_id = pi_exp_bl_id
                         and i.imfi_id = pi_imfi_id
                         and sh.shss_exp_voyage_code <> i.imfi_voyage
                      union
                      select 'CSHC_EN_VESSEL' as colname,
                             ship.cshc_en_vessel as colvalue1,
                             i.imfi_vessel_name as colvalue2,
                             null as seqno,
                             null as cntno,
                             '0' as tbltype
                        from sexportmanifest s, shsailingschedule sh, cshipcanonical ship, imanifestin i
                       where s.ssem_voyage_id = sh.shss_voyage_id
                         and sh.shss_vessel_code = ship.cshc_id
                         and s.ssem_exp_bl_id = pi_exp_bl_id
                         and i.imfi_id = pi_imfi_id
                         and ship.cshc_en_vessel <> i.imfi_vessel_name
                      union
                      select 'SPGI_QUANTITY' as cloname,
                             to_char(spgi.spgi_quantity) as colvalue1,
                             to_char(imai.imai_cargo_package_number) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype
                        from scargoinfo spgi, (select sum(i.imai_cargo_package_number) imai_cargo_package_number
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                         and spgi.spgi_quantity <> imai.imai_cargo_package_number
                      union
                      select 'SPGI_MEASUREMENT' as cloname,
                             to_char(spgi.spgi_measurement) as colvalue1,
                             to_char(imai.imai_cargo_measurement) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype
                        from scargoinfo spgi, (select sum(i.imai_cargo_measurement) imai_cargo_measurement
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                         and spgi.spgi_measurement <> imai.imai_cargo_measurement
                      union
                      select 'SPGI_GROSS_WEIGHT' as cloname,
                             to_char(spgi.spgi_gross_weight) as colvalue1,
                             to_char(imai.imai_cargo_gross_weight) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype
                        from scargoinfo spgi, (select sum(i.imai_cargo_gross_weight) imai_cargo_gross_weight
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                         and spgi.spgi_gross_weight <> imai.imai_cargo_gross_weight
                      union
                      select 'SPGI_CARGO_DESCRIPTION_EN' as cloname,
                             trim(replace(replace(spgi.spgi_cargo_description_en,chr(10),' '),chr(13),' ')) as colvalue1,
                             substrb(trim(replace(replace(imai_cargo_description,chr(10),' '),chr(13),' ')), 1, 2000) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype
                        from scargoinfo spgi, (select imai_cargo_description
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id
                                                  and rownum = 1) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                         and replace(replace(replace(spgi.spgi_cargo_description_en,chr(10),''),chr(13),''),' ') <>
                             substrb(replace(replace(replace(imai_cargo_description,chr(10),''),chr(13),''),' '), 1, 2000)
                      union
                      select 'SPCI_CNT_NO' as cloname,
                             null as colvalue1,
                             imti.imti_container_no as colvalue2,
                             null as seqno,
                             imti.imti_container_no as cntno,
                             '1' as tbltype
                        from imanifestincontainer imti
                       where imti.imti_pid = pi_imfi_id
                         and imti.imti_container_no not in
                             (select spci.spci_cnt_no
                                from scontainerinfo spci
                               where spci.spci_exp_bl_id = pi_exp_bl_id)
                      union
                      select 'SPCI_CNT_NO' as cloname,
                             spci.spci_cnt_no as colvalue1,
                             null as colvalue2,
                             null as seqno,
                             spci.spci_cnt_no as cntno,
                             '1' as tbltype
                        from scontainerinfo spci
                       where spci.spci_exp_bl_id = pi_exp_bl_id
                         and spci.spci_cnt_no not in
                             (select imti.imti_container_no
                                from imanifestincontainer imti
                               where imti.imti_pid = pi_imfi_id)
                      union
                      select 'SPCI_SEAL_NO' as cloname,
                             spci.spci_seal_no as colvalue1,
                             (select imsi.imsi_seal_no
                                from imanifestinseal imsi
                               where imsi.imsi_pid = imti.imti_id
                                 and rownum = 1) as colvalue2,
                             null as seqno,
                             spci.spci_cnt_no as cntno,
                             '1' as tbltype
                        from scontainerinfo spci, imanifestincontainer imti
                       where spci.spci_cnt_no = imti.imti_container_no
                         and spci.spci_exp_bl_id = pi_exp_bl_id
                         and imti.imti_pid = pi_imfi_id
                         and spci.spci_seal_no <> (select imsi.imsi_seal_no
                                                     from imanifestinseal imsi
                                                    where imsi.imsi_pid = imti.imti_id
                                                      and rownum = 1)) loop
        i := i + 1;
        pi_compare_col(i).colname := l_record.colname;
        pi_compare_col(i).colvalue1 := l_record.colvalue1;
        pi_compare_col(i).colvalue2 := l_record.colvalue2;
        pi_compare_col(i).seqno := l_record.seqno;
        pi_compare_col(i).cntno := l_record.cntno;
        pi_compare_col(i).tbltype := l_record.tbltype;
     end loop;
  end getCompareCol;
  
 /**
  * �Ƚϵ�����ᵥ��ϵͳ��ԭ���ᵥ�Ĳ�����Ϣ
  * ���Ѳ�����Ϣ����IIFTMBCFORS��
  */
  procedure insertDiffRecord(pi_org_id     in varchar2,
                             pi_batch_no   in varchar2,
                             pi_imfRow     in imanifestin%rowtype,
                             pi_colname    in varchar2,
                             pi_colvalue1  in varchar2,
                             pi_colvalue2  in varchar2,
                             pi_seqno      in varchar2,
                             pi_cntno      in varchar2,
                             pi_tbltype    in char,
                             pi_resultcode in varchar2,
                             pi_resultdesc in varchar2) as
  begin
    insert into iiftmbcfors
          (iifs_id,
           iifs_org_id,
           iifs_create_time,
           iifs_batch_no,
           iifs_send_flag,
           iifs_receiver_code,
           iifs_file_type,
           iifs_bl_no,
           iifs_vessel_code,
           iifs_voyage,
           iifs_vessel_name,
           iifs_load_port_code,
           iifs_discharge_port_code,
           iifs_column_name,
           iifs_old_value,
           iifs_new_value,
           iifs_seq_no,
           iifs_container_no,
           iifs_type,
           iifs_ie_flag,
           iifs_result_code,
           iifs_result_desc)
          select seq_iifs_id.nextval,
                 pi_org_id,
                 sysdate,
                 pi_batch_no,
                 'N',
                 pi_imfRow.Imfi_Rs_Code,
                 pi_imfRow.Imfi_File_Type,
                 pi_imfRow.Imfi_Bl_No,
                 nvl(pi_imfRow.Imfi_Vessel_Code,
                     (select c.cshc_id
                        from cshipcanonical c 
                       where c.cshc_en_vessel = pi_imfRow.Imfi_Vessel_Name
                         and c.cshc_org_id = pi_org_id
                         and c.cshc_cancel_flag = 'N'
                         and rownum = 1)),
                 pi_imfRow.Imfi_Voyage,
                 pi_imfRow.Imfi_Vessel_Name,
                 pi_imfRow.Imfi_Load_Port_Code,
                 pi_imfRow.Imfi_Discharge_Port_Code,
                 pi_colname,
                 substr(pi_colvalue1,1,2000),
                 substr(pi_colvalue2,1,2000),
                 pi_seqno,
                 pi_cntno,
                 pi_tbltype,
                 'E',
                 pi_resultcode,
                 pi_resultdesc
          from dual;
  end insertDiffRecord;

 /**
  * �Ƚϵ�����ᵥ��ϵͳ��ԭ���ᵥ�Ĳ�����Ϣ
  * ���Ѳ�����Ϣ����IIFTMBCFORS��
  */
  procedure compareBlData(pi_exp_bl_id in varchar2,
                          pi_org_id    in varchar2,
                          pi_batch_no  in varchar2,
                          pi_imfRow    in imanifestin%rowtype) as
  l_compare_col t_col;
  begin
    getCompareCol(pi_exp_bl_id, pi_imfRow.Imfi_Id, l_compare_col);
    for i in 1 .. l_compare_col.count loop
      insertDiffRecord(pi_org_id,
                       pi_batch_no,
                       pi_imfRow,
                       l_compare_col(i).colname,
                       l_compare_col(i).colvalue1,
                       l_compare_col(i).colvalue2,
                       l_compare_col(i).seqno,
                       l_compare_col(i).cntno,
                       l_compare_col(i).tbltype,
                       l_compare_col(i).resultcode,
                       l_compare_col(i).resultdesc);
    end loop;
  end compareBlData;  
  
  procedure getCompareCol2(pi_exp_bl_id   in varchar2,
                          pi_imfi_id     in varchar2,
                          pi_compare_col out t_col)  as
    i int := 0;
    begin
    for l_record in (select 'SPGI_QUANTITY' as cloname,
                             to_char(spgi.spgi_quantity) as colvalue1,
                             to_char(imai.imai_cargo_package_number) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             decode(spgi.spgi_quantity,imai.imai_cargo_package_number,'0','1') as resultCode,
                             decode(spgi.spgi_quantity,imai.imai_cargo_package_number,'','�ܼ����в���') as resultDesc
                        from scargoinfo spgi, (select sum(i.imai_cargo_package_number) imai_cargo_package_number
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_MEASUREMENT' as cloname,
                             to_char(spgi.spgi_measurement) as colvalue1,
                             to_char(imai.imai_cargo_measurement) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             decode(spgi.spgi_measurement,imai.imai_cargo_measurement,'0','1') as resultCode,
                             decode(spgi.spgi_measurement,imai.imai_cargo_measurement,'','������в���') as resultDesc
                        from scargoinfo spgi, (select sum(i.imai_cargo_measurement) imai_cargo_measurement
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_GROSS_WEIGHT' as cloname,
                             to_char(spgi.spgi_gross_weight) as colvalue1,
                             to_char(imai.imai_cargo_gross_weight) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             decode(spgi.spgi_gross_weight,imai.imai_cargo_gross_weight,'0','1') as resultCode,
                             decode(spgi.spgi_gross_weight,imai.imai_cargo_gross_weight,'','�������в���') as resultDesc
                        from scargoinfo spgi, (select sum(i.imai_cargo_gross_weight) imai_cargo_gross_weight
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_CARGO_DESCRIPTION_EN' as cloname,
                             trim(replace(replace(spgi.spgi_cargo_description_en,chr(10),' '),chr(13),' ')) as colvalue1,
                             substrb(trim(replace(replace(imai_cargo_description,chr(10),' '),chr(13),' ')), 1, 2000) as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             decode(replace(replace(replace(spgi.spgi_cargo_description_en,chr(10),''),chr(13),''),' '),
                                    substrb(replace(replace(replace(imai_cargo_description,chr(10),''),chr(13),''),' '), 1, 2000),'0','0') as resultCode,
                             decode(replace(replace(replace(spgi.spgi_cargo_description_en,chr(10),''),chr(13),''),' '),
                                    substrb(replace(replace(replace(imai_cargo_description,chr(10),''),chr(13),''),' '), 1, 2000),'','Ʒ���в���') as resultDesc
                        from scargoinfo spgi, (select imai_cargo_description
                                                 from imanifestincargo i 
                                                where i.imai_pid = pi_imfi_id
                                                  and rownum = 1) imai
                       where spgi.spgi_exp_bl_id = pi_exp_bl_id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPCI_CNT_NO' as cloname,
                             cnt.cnt_no as colvalue1,
                             icnt.icnt_no as colvalue2,
                             null as seqno,
                             null as cntno,
                             '1' as tbltype,
                             decode(cnt.cnt_no,icnt.icnt_no,'0','1') as resultCode,
                             decode(cnt.cnt_no,icnt.icnt_no,'','����в���') as resultDesc
                        from (select f_link(spci.spci_cnt_no || ' ' ) cnt_no
                                from scontainerinfo spci
                               where spci.spci_exp_bl_id = pi_exp_bl_id
                               order by spci.spci_cnt_no asc) cnt,
                              (select f_link(imti.imti_container_no || ' ' ) icnt_no
                                 from imanifestincontainer imti 
                                where imti.imti_pid = pi_imfi_id) icnt
                      union
                      select 'SPCI_SEAL_NO' as cloname,
                             spci.spci_seal_no as colvalue1,
                             imt.imsi_seal_no as colvalue2,
                             null as seqno,
                             spci.spci_cnt_no as cntno,
                             '1' as tbltype,
                             decode(spci.spci_seal_no,imt.imsi_seal_no,'0','1') as resultCode,
                             decode(spci.spci_seal_no,imt.imsi_seal_no,'','Ǧ����в���') as resultDesc
                        from scontainerinfo spci
                         left join
                            (select imti.imti_container_no ,
                                    (select imsi.imsi_seal_no 
                                        from imanifestinseal imsi 
                                       where imsi.imsi_pid = imti.imti_id and rownum = 1) imsi_seal_no
                               from imanifestincontainer imti 
                              where imti.imti_pid = pi_imfi_id) imt 
                          on spci.spci_cnt_no = imt.imti_container_no
                       where spci.spci_exp_bl_id = pi_exp_bl_id
                     union
                      select 'SPCI_SEAL_NO' as cloname,
                             spci.spci_seal_no as colvalue1,
                             imt.imsi_seal_no as colvalue2,
                             null as seqno,
                             imt.imti_container_no as cntno,
                             '1' as tbltype,
                             decode(spci.spci_seal_no,imt.imsi_seal_no,'0','1') as resultCode,
                             decode(spci.spci_seal_no,imt.imsi_seal_no,'','Ǧ����в���') as resultDesc
                        from (select imti.imti_container_no,
                                     (select imsi.imsi_seal_no 
                                        from imanifestinseal imsi 
                                       where imsi.imsi_pid = imti.imti_id and rownum = 1) imsi_seal_no
                                from imanifestincontainer imti 
                               where imti.imti_pid = pi_imfi_id) imt 
                         left join scontainerinfo spci
                           on spci.spci_cnt_no = imt.imti_container_no
                          and spci.spci_exp_bl_id = pi_exp_bl_id) loop
        i := i + 1;
        pi_compare_col(i).colname := l_record.cloname;
        pi_compare_col(i).colvalue1 := l_record.colvalue1;
        pi_compare_col(i).colvalue2 := l_record.colvalue2;
        pi_compare_col(i).seqno := l_record.seqno;
        pi_compare_col(i).cntno := l_record.cntno;
        pi_compare_col(i).tbltype := l_record.tbltype;
        pi_compare_col(i).resultCode := l_record.resultCode;
        pi_compare_col(i).resultDesc := l_record.resultDesc;
     end loop;
  end getCompareCol2;
  
  /**
  * �Ƚϵ�����ᵥ��ϵͳ��ԭ���ᵥ�Ĳ�����Ϣ
  * ���Ѳ�����Ϣ����IIFTMBCFORS��
  */
  procedure compareBlData2(pi_exp_bl_id in varchar2,
                          pi_org_id    in varchar2,
                          pi_batch_no  in varchar2,
                          pi_imfRow    in imanifestin%rowtype) as
  l_compare_col t_col;
  begin
    getCompareCol2(pi_exp_bl_id, pi_imfRow.Imfi_Id, l_compare_col);

    for i in 1 .. l_compare_col.count loop
      insertDiffRecord(pi_org_id,
                       pi_batch_no,
                       pi_imfRow,
                       l_compare_col(i).colname,
                       l_compare_col(i).colvalue1,
                       l_compare_col(i).colvalue2,
                       l_compare_col(i).seqno,
                       l_compare_col(i).cntno,
                       l_compare_col(i).tbltype,
                       l_compare_col(i).resultcode,
                       l_compare_col(i).resultdesc);
    end loop;
  end compareBlData2;
  
  --�ᵥ������Ϣ
  procedure compareBlMoreInfo(pi_verify_batch_no in varchar2,
                              pi_org_id          in varchar2,
                              pi_imfRow          in imanifestin%rowtype) is
  begin                       
      for l_record in (select 'SPGI_QUANTITY' as cloname,
                             '' as colvalue1,
                             sum(imai.imai_cargo_package_number) || '' as colvalue2,
                             '' as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ��ܼ���' as resultDesc
                        from imanifestincargo imai
                       where imai.imai_pid = pi_imfRow.Imfi_Id
                      union
                      select 'SPGI_MEASUREMENT' as cloname,
                             '' as colvalue1,
                             sum(imai.imai_cargo_measurement) || '' as colvalue2,
                             '' as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ������' as resultDesc
                        from imanifestincargo imai
                       where imai.imai_pid = pi_imfRow.Imfi_Id
                      union
                      select 'SPGI_GROSS_WEIGHT' as cloname,
                             '' as colvalue1,
                             sum(imai.imai_cargo_gross_weight) || '' as colvalue2,
                             '' as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ�������' as resultDesc
                        from imanifestincargo imai
                       where imai.imai_pid = pi_imfRow.Imfi_Id
                      union
                      select 'SPGI_CARGO_DESCRIPTION_EN' as cloname,
                             '' as colvalue1,
                             trim(replace(replace(imai.imai_cargo_description,chr(10),' '),chr(13),' ')) as colvalue2,
                             '' as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ�Ʒ��' as resultDesc
                        from imanifestincargo imai
                       where imai.imai_pid = pi_imfRow.Imfi_Id
                         and rownum = 1
                      union
                      select 'SPCI_CNT_NO' as cloname,
                             '' as colvalue1,
                             f_link(imti.imti_container_no || ' ' ) as colvalue2,
                             null as seqno,
                             null as cntno,
                             '1' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ����' as resultDesc
                        from imanifestincontainer imti
                       where imti.imti_pid = pi_imfRow.Imfi_Id
                      union
                      select 'SPCI_SEAL_NO' as cloname,
                             '' as colvalue1,
                             f_link(imsi.imsi_seal_no || ' ' ) as colvalue2,
                             null as seqno,
                             '' as cntno,
                             '1' as tbltype,
                             '1' as resultCode,
                             '�յ���ȱ�ٵ�Ǧ���' as resultDesc
                        from imanifestinseal imsi
                       where imsi.imsi_imfi_id = pi_imfRow.Imfi_Id) loop
        insertDiffRecord(pi_org_id,pi_verify_batch_no,pi_imfRow,l_record.cloname,l_record.colvalue1,l_record.colvalue2,
             l_record.seqno,l_record.cntno, l_record.tbltype,l_record.resultCode,l_record.resultDesc);
      end loop;
  end compareBlMoreInfo;
  
  procedure compareManifestMoreInfo(pi_batch_no        in varchar2,
                                    pi_verify_batch_no in varchar2,
                                    pi_org_id          in varchar2) is
    l_imfRow          imanifestin%rowtype;
  begin
    insert into temp_id(id, id_type, id_type_2, id_type_3)
    select sef.ssem_exp_bl_id,t.id_type,t.id_type_2,'MANIFEST_VERIFY_BLNO'
      from sexportmanifest sef, temp_id t
     where sef.ssem_org_id = pi_org_id
       and sef.ssem_document_type = '0'
       and sef.ssem_voyage_id = t.id
       and sef.ssem_fcl_lcl_flag = 'F'
       and t.id_type_3 = 'MANIFEST_VERIFY_VOYAGE'
       and not exists
           (select 0
              from imanifestin imf, shsailingschedule shss, cshipcanonical c
             where imf.imfi_batch_no = pi_batch_no
               and imf.imfi_vessel_code = c.cshc_id
               and imf.imfi_voyage = shss.shss_exp_voyage_code
               --and imf.imfi_carrier_code = sef.ssem_carrier_id
               and imf.imfi_bl_no = sef.ssem_bl_no
               and shss.shss_voyage_id = sef.ssem_voyage_id
               and shss.shss_vessel_code = c.cshc_id);
    for l_temp in (select id, id_type,id_type_2,id_type_3 from temp_id t where t.id_type_3 = 'MANIFEST_VERIFY_BLNO') loop
      begin
      select (select i.imfi_rs_code from imanifestin i where i.imfi_batch_no = pi_batch_no and i.imfi_rs_code is not null and rownum = 1),
             (select i.imfi_file_type from imanifestin i where i.imfi_batch_no = pi_batch_no and i.imfi_file_type is not null and rownum = 1),
             s.ssem_bl_no,
             l_temp.id_type,
             l_temp.id_type_2,
             (select c.cshc_en_vessel from cshipcanonical c where c.cshc_id = l_temp.id_type),
             s.ssem_load_port_code,
             s.ssem_discharge_port_code
        into l_imfRow.Imfi_Rs_Code,
             l_imfRow.Imfi_File_Type,
             l_imfRow.Imfi_Bl_No,
             l_imfRow.Imfi_Vessel_Code,
             l_imfRow.Imfi_Voyage,
             l_imfRow.Imfi_Vessel_Name,
             l_imfRow.Imfi_Load_Port_Code,
             l_imfRow.Imfi_Discharge_Port_Code
        from sexportmanifest s
       where s.ssem_exp_bl_id = l_temp.id;
      exception
        when others then
          null;
      end;
      insertDiffRecord(pi_org_id,pi_verify_batch_no,l_imfRow,
                       'SSEM_BL_NO',l_imfRow.imfi_bl_no,'',null,null, 0,'1','�òյ������Ҳ�����Ӧ�Ĵ���˾�ᵥ');
                       
      for l_record in (select 'SPGI_QUANTITY' as cloname,
                             to_char(spgi.spgi_quantity) as colvalue1,
                             '' as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥ�ܼ���' as resultDesc
                        from scargoinfo spgi
                       where spgi.spgi_exp_bl_id = l_temp.id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_MEASUREMENT' as cloname,
                             to_char(spgi.spgi_measurement) as colvalue1,
                             '' as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥ�����' as resultDesc
                        from scargoinfo spgi
                       where spgi.spgi_exp_bl_id = l_temp.id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_GROSS_WEIGHT' as cloname,
                             to_char(spgi.spgi_gross_weight) as colvalue1,
                             '' as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥ������' as resultDesc
                        from scargoinfo spgi
                       where spgi.spgi_exp_bl_id = l_temp.id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPGI_CARGO_DESCRIPTION_EN' as cloname,
                             trim(replace(replace(spgi.spgi_cargo_description_en,chr(10),' '),chr(13),' ')) as colvalue1,
                             '' as colvalue2,
                             spgi.spgi_seq_no as seqno,
                             null as cntno,
                             '2' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥƷ��' as resultDesc
                        from scargoinfo spgi
                       where spgi.spgi_exp_bl_id = l_temp.id
                         and spgi.spgi_record_type = 1
                      union
                      select 'SPCI_CNT_NO' as cloname,
                             f_link(spci.spci_cnt_no || ' ' ) as colvalue1,
                             '' as colvalue2,
                             null as seqno,
                             null as cntno,
                             '1' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥ���' as resultDesc
                        from scontainerinfo spci
                       where spci.spci_exp_bl_id = l_temp.id
                      union
                      select 'SPCI_SEAL_NO' as cloname,
                             f_link(spci.spci_seal_no || ' ' ) as colvalue1,
                             '' as colvalue2,
                             null as seqno,
                             '' as cntno,
                             '1' as tbltype,
                             '1' as resultCode,
                             '����˾�ᵥ��ȱ�ٵ��ᵥǦ���' as resultDesc
                        from scontainerinfo spci
                       where spci.spci_exp_bl_id = l_temp.id) loop
        insertDiffRecord(pi_org_id,pi_verify_batch_no,l_imfRow,l_record.cloname,l_record.colvalue1,l_record.colvalue2,
             l_record.seqno,l_record.cntno, l_record.tbltype,l_record.resultCode,l_record.resultDesc);
      end loop;
    end loop;
  end compareManifestMoreInfo;
  
  --�����ᵥ�˶Ա���
  procedure expMfVerifyReport(pi_batch_no  in varchar2,
                              pi_org_id    in varchar2,
                              pi_check_type in varchar2,
                              po_result    out sys_refcursor)is
    l_imfRow     imanifestin%rowtype;
    l_voyage_id  sexportmanifest.ssem_voyage_id%type;
    l_exp_bl_id  sexportmanifest.ssem_exp_bl_id%type;
    l_com_batch_no iiftmbcfors.iifs_batch_no%type;
     cursor cur_imfi is
       select imf.imfi_id,imf.imfi_bl_no,imf.imfi_vessel_code,
              imf.imfi_vessel_name,imf.imfi_voyage,imf.imfi_load_port_code,
              imf.imfi_rs_code,imf.imfi_file_type,imf.imfi_discharge_port_code,
              imf.imfi_carrier_code
         from imanifestin imf 
        where imf.imfi_batch_no = pi_batch_no
          and nvl(imf.imfi_dispose_flag,'N') = 'N';
  begin
    select seq_iifs_seqid.nextval into l_com_batch_no from dual; --MSK���ϲյ��Ա����κ�
    open cur_imfi;
    loop
      <<nextRecord>>
      fetch cur_imfi
       into l_imfRow.Imfi_Id,l_imfRow.Imfi_Bl_No,l_imfRow.Imfi_Vessel_Code,
            l_imfRow.Imfi_Vessel_Name,l_imfRow.Imfi_Voyage,l_imfRow.Imfi_Load_Port_Code,
            l_imfRow.Imfi_Rs_Code,l_imfRow.Imfi_File_Type,l_imfRow.Imfi_Discharge_Port_Code,
            l_imfRow.Imfi_Carrier_Code;
      exit when cur_imfi%notfound;
      
      begin
        l_voyage_id := getVoyageId(pi_org_id,l_imfRow);
      exception
        when others then
          l_voyage_id := null;
      end;
      
      if l_voyage_id is null then
        begin
          select distinct sh.shss_voyage_id
            into l_voyage_id
            from shsailingschedule sh, shvoyageslotinfo svp ,cshipcanonical c
           where sh.shss_delete_flag = 'N'
             and sh.shss_cancel_flag = 'N'
             and sh.shss_voyage_id = svp.shvs_voyage_id
             and svp.shvs_imp_exp_flag = 'E'
             and svp.shvs_slotowner_id = l_imfRow.Imfi_Carrier_Code
             and svp.shvs_carrier_voyage_code = l_imfRow.Imfi_Voyage
             and sh.shss_org_id = pi_org_id
             and sh.shss_vessel_code = c.cshc_id
             and c.cshc_en_vessel = l_imfRow.Imfi_Vessel_Name
             and c.cshc_org_id = pi_org_id
             and c.cshc_cancel_flag = 'N';
        exception
          when others then
            raise_application_error(-20005, '����(����):' || l_imfRow.Imfi_Vessel_Name || '(' ||
                                    l_imfRow.Imfi_Vessel_Code || '),���ں���:' ||
                                    l_imfRow.Imfi_Voyage || '�ڴ���û��ά��' || chr(13) ||' ## ');
        end;
      end if;
      
      if l_voyage_id is null then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ����ƥ�䴬����'||l_imfRow.Imfi_Vessel_Name ||'����:'||l_imfRow.Imfi_Voyage);
        goto nextRecord;
      end if;
      
      if pi_check_type = 2 then
      --��¼����ID
        insert into temp_id(id,id_type,id_type_2,id_type_3)
         select sh.shss_voyage_id,c.cshc_id,sh.shss_exp_voyage_code,'MANIFEST_VERIFY_VOYAGE' 
           from shsailingschedule sh, cshipcanonical c
          where sh.shss_voyage_id = l_voyage_id and sh.shss_vessel_code = c.cshc_id
            and not exists(select 0 from temp_id t  where t.id = sh.shss_voyage_id and t.id_type_3 = 'MANIFEST_VERIFY_VOYAGE');
      end if;
                   
      l_exp_bl_id := getManifestId(l_imfRow.Imfi_Bl_No,l_voyage_id,pi_org_id);
      if l_exp_bl_id = -1 or l_exp_bl_id = -2 then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ�޷�ƥ���ᵥ���ᵥ�ظ�');
        if pi_check_type = 2 then
          insertDiffRecord(pi_org_id,l_com_batch_no,l_imfRow,
                           'SSEM_BL_NO','',l_imfRow.Imfi_Bl_No,null,null, 0,'1','�ô���˾�ᵥ�Ҳ�����Ӧ�Ĳյ�����');
          compareBlMoreInfo(l_com_batch_no, pi_org_id,l_imfRow);
        else
          insertDiffRecord(pi_org_id,l_com_batch_no,l_imfRow,
                           'SSEM_BL_NO','�յ���ȱ�ٵ��ᵥ��','',null,null, 0,'1','�յ���ȱ�ٵ��ᵥ��');
        end if;
        goto nextRecord;
      else
        if pi_check_type = 2 then
          compareBlData2(l_exp_bl_id, pi_org_id, l_com_batch_no, l_imfRow);
        else 
          compareBlData(l_exp_bl_id, pi_org_id, l_com_batch_no, l_imfRow);
        end if;
        updateImfiDipose(l_imfRow.Imfi_Id,'Y','�ᵥ�˶Գɹ�;');
      end if;
    end loop;
    close cur_imfi;
    
    --�ȶԲյ������ᵥ��Ϣ
    if pi_check_type = 2 then
      compareManifestMoreInfo(pi_batch_no,l_com_batch_no, pi_org_id);
    end if;
    
    --����˶Խ����
    open po_result for
          select imf.imfi_bl_no as BL_NO ,
             imf.imfi_dispose_flag as IMPORT_FLAG,
             imf.imfi_err_reason   as IMPORT_INFO,
             imf.imfi_batch_no     as BATCH_NO
        from imanifestin imf
       where imf.imfi_batch_no = pi_batch_no;
  end expMfVerifyReport;
  
  
  --�˶Բյ�
  procedure verifyExpManifest(pi_batch_no  in varchar2,
                              pi_org_id    in varchar2,
                              po_result    out sys_refcursor)is
    l_imfRow     imanifestin%rowtype;
    l_rs_code    imanifestin.imfi_rs_code%type;
    l_voyage_id  sexportmanifest.ssem_voyage_id%type;
    l_exp_bl_id  sexportmanifest.ssem_exp_bl_id%type;
    
    l_paramsValue  saparamsdetail%rowtype;
    l_message    varchar2(1000);
    cursor cur_imfi is
       select imf.imfi_id,imf.imfi_bl_no,imf.imfi_vessel_code,
              imf.imfi_vessel_name,imf.imfi_voyage,imf.imfi_load_port_code
         from imanifestin imf 
        where imf.imfi_batch_no = pi_batch_no
          and nvl(imf.imfi_dispose_flag,'N') = 'N';
  begin
    begin
      select imf.imfi_rs_code
        into l_rs_code
        from imanifestin imf 
       where imf.imfi_batch_no = pi_batch_no
         and rownum = 1;
    end;
    --���ɱȶԱ���
    getSaparamsConfig('SADO_EXP_GEN_VERIFY_REPORT',pi_org_id,l_paramsValue);
    if l_paramsValue.Sapd_Value1 = 'Y' and 
      (l_paramsValue.Sapd_Value2 is null or instr(l_paramsValue.Sapd_Value2 ,l_rs_code) > 0)  then
      expMfVerifyReport(pi_batch_no,pi_org_id,l_paramsValue.Sapd_Value3,po_result);
      return;
    end if;
    delete from temp_id;
    open cur_imfi;
    loop
      <<nextRecord>>
      fetch cur_imfi
       into l_imfRow.Imfi_Id,l_imfRow.Imfi_Bl_No,l_imfRow.Imfi_Vessel_Code,
            l_imfRow.Imfi_Vessel_Name,l_imfRow.Imfi_Voyage,l_imfRow.Imfi_Load_Port_Code;
      exit when cur_imfi%notfound;
      begin
        l_voyage_id := getVoyageId(pi_org_id,l_imfRow);
      exception
        when others then
          l_voyage_id := null;
      end;
      if l_voyage_id is null then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ����ƥ�䴬����'||l_imfRow.Imfi_Vessel_Name ||'����:'||l_imfRow.Imfi_Voyage);
        goto nextRecord;
      end if;
      
      --��¼����ID
      insert into temp_id(id,id_type,id_type_2)
       select l_voyage_id,'VOYAGEID','MANIFEST_VERIFY' from dual
        where not exists(select 0 from temp_id t 
                 where t.id = l_voyage_id 
                   and t.id_type = 'VOYAGEID' 
                   and t.id_type_2 = 'MANIFEST_VERIFY');
                   
      l_exp_bl_id := getManifestId(l_imfRow.Imfi_Bl_No,l_voyage_id,pi_org_id);
      if l_exp_bl_id = -1 or l_exp_bl_id = -2 then
        updateImfiDipose(l_imfRow.Imfi_Id,'E','ϵͳ�޷�ƥ���ᵥ���ᵥ�ظ�');
        goto nextRecord;
      end if;
      
      verifyTotalCargoInfo(l_imfRow.Imfi_Id,l_exp_bl_id,l_message);
      
      if l_message is not null then
        updateImfiDipose(l_imfRow.Imfi_Id,'E',l_message);
      else
        updateImfiDipose(l_imfRow.Imfi_Id,'Y','�˶��޲���');
      end if;
    end loop;
    close cur_imfi;
       
    open po_result for
      select f_link(BL_NO||',') BL_NO ,IMPORT_FLAG ,IMPORT_INFO ,BATCH_NO
        from (select sef.ssem_bl_no  as BL_NO,
                     'N' as IMPORT_FLAG,
                     'ϵͳ����δ�˶��ᵥ'  as IMPORT_INFO,
                     pi_batch_no     as BATCH_NO
                from sexportmanifest sef ,temp_id t
               where sef.ssem_org_id = pi_org_id
                 and sef.ssem_document_type = '0'
                 and sef.ssem_voyage_id = t.id
                 and sef.ssem_fcl_lcl_flag = 'F' --HY3-1142 ����Ԥ�丱��
                 and t.id_type = 'VOYAGEID'
                 and t.id_type_2 = 'MANIFEST_VERIFY'
                 and not exists
                   (select 0 from imanifestin imf ,shsailingschedule shss ,cshipcanonical c
                     where imf.imfi_batch_no = pi_batch_no
                       and imf.imfi_vessel_code = c.cshc_id
                       and imf.imfi_voyage = shss.shss_exp_voyage_code
                       and imf.imfi_carrier_code = sef.ssem_carrier_id
                       and imf.imfi_bl_no = sef.ssem_bl_no
                       and shss.shss_voyage_id = sef.ssem_voyage_id
                       and shss.shss_vessel_code = c.cshc_id)
                 order by sef.ssem_bl_no)
       group by IMPORT_FLAG ,IMPORT_INFO ,BATCH_NO
    union all
      select imf.imfi_bl_no as BL_NO ,
             imf.imfi_dispose_flag as IMPORT_FLAG,
             imf.imfi_err_reason   as IMPORT_INFO,
             imf.imfi_batch_no     as BATCH_NO
        from imanifestin imf
       where imf.imfi_batch_no = pi_batch_no;
  end verifyExpManifest;
end EDI_EXP_MANIFEST_VERIFY_IN;
/
