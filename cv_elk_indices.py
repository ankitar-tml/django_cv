
class elk_indices():
    #Vin Details
    def vin_details(self):
        index="flat_mfg_vin_details"
        return index
    
    #Plan VS Actual
    def plan_vs_actual_digital(self):
        #index_digital="digital_vin_test" 
        index_digital="flat_mfg_digital_twin_api"
        return index_digital
    
    def plan_vs_actual(self):
        #index="sequence_adherence_test2"
        index="flat_mfg_produced_material_v1_kafka"
        return index
    
    def plan_vs_actual_vc_description(self):
        index="flat_mfg_cv_vc_description"
        return index

    def plan_vs_actual_status(self):
        index="status_index"
        #index="flat_mfg_plan_vs_actual_shoptype_status"
        return index
    
    #TAT
    def TAT_store_dock(self):
        index_store_dock="flat_mfg_store_dock_master"
        return index_store_dock
    
    def TAT(self):
        index="flat_mfg_uass_tat"
        return index
    
    #Reconciliation 
    def Reconciliation(self):
        index="flat_mfg_cv_reconciliation_transitions"
        return index
    
    def Reconciliation_cnt_sum(self):
        index="flat_mfg_cv_count_sum"
        return index
    
    def Reconciliation_status_transitions(self):
        index="flat_mfg_cv_reconciliation_status_wise_count_transitions"
        return index