import numpy as np
from fuzzywuzzy import fuzz

class ClinicalStudiesMatcher(object):
    
    def __init__(self, df_1, df_2):
        self.df_1 = df_1
        self.df_2 = df_2
        
        
    def matches_date_range(self, study_sd, study_cd, study_pcd, p_fd, p_gd):
        if study_sd and (p_fd < study_sd < p_gd):
            return True
        elif study_cd and (p_fd < study_cd < p_gd):
            return True
        elif study_pcd and (p_fd < study_pcd < p_gd):
            return True
        else:
            return False
            

    def official_title_matches_patent_title(self, stdy_title, patent_title):
        if stdy_title and patent_title:
            return fuzz.token_set_ratio(stdy_title, patent_title)
        else:
            return False

    def source_matches_nih_org(self, src, nih_org):
        if src and nih_org:
            if ';' in str(nih_org):
                orgs_list = str(nih_org).split(';')
                if src in orgs_list:
                    return True
                else:
                    return False
            else:
                if src in str(nih_org):
                    return True
                else:
                    return False
        else:
            return False

    def source_matches_fda_applicant(self, src, fda_org):
        if src and fda_org:
            if ';' or '|' in str(fda_org):
                l = str(fda_org).replace(';','|').split('|')
                if src in l:
                    return True
                else:
                    return False
            else:
                if src in str(fda_org):
                    return True
                else:
                    return False
        else:
            return False

    def is_drug_inventive(self, intervention_type, dc_bool):
        if intervention_type and dc_bool:
            if (intervention_type == 'Drug') and (dc_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_device_inventive(self, intervention_type, dsd_bool):
        if (intervention_type and dsd_bool):
            if (intervention_type == 'Device') and (dsd_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_radiation_inventive(self, intervention_type, rm_bool):
        if intervention_type and rm_bool:
            if (intervention_type == 'Radiation') and (rm_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_food_and_nutrition_inventive(self, intervention_type, fn_bool):
        if intervention_type and fn_bool:
            if (intervention_type == 'Dietary Supplement') and (fn_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_cell_inventive(self, intervention_type, c_bool):
        if intervention_type and c_bool:
            if (intervention_type == 'Biological') and (c_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_other_inventive(self, intervention_type, o_bool):
        if intervention_type and o_bool:
            if (intervention_type == 'Other') and (o_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def is_dna_inventive(self, intervention_type, dna_bool):
        if intervention_type and dna_bool:
            if (intervention_type == 'Genetic') and (dna_bool == 1):
                return True
            else:
                return False
        else:
            return False

    def contains_mesh_term_in_fda_drug_trade_name(self, m_term, trade_name):
        if m_term and trade_name:
            if ';' or '|' in str(trade_name):
                l = str(trade_name).replace(';','|').split('|')
                if m_term in l:
                    return True
                else:
                    return False
            else:
                if m_term in str(trade_name):
                    return True
                else:
                    return False
        else:
            return False

    def contains_mesh_term_in_fda_ingredient(self, m_term, ingredient):
        if m_term and ingredient:
            if ';' or '|' in str(ingredient):
                l = str(ingredient).replace(';','|').split('|')
                if m_term in l:
                    return True
                else:
                    return False
            else:
                if m_term in str(ingredient):
                    return True
                else:
                    return False
        else:
            return False

    def is_nci_funded(self, nci_bool, nih_grant_number):
        if nci_bool and nih_grant_number:
            if nci_bool == 'Yes':
                return True
            else:
                return False
        else:
            return False

        
    def calc_results(self, d):
        score = 0
        if (d['matches_date_range']):
            score = score + 100
        if (d['matches_drug'] or d['matches_device'] or d['matches_radiation'] or d['matches_dietary'] or d['matches_biological'] or d['matches_genetic'] or d['matches_other']):
            score = score + 100
        score = score + int(d['title_value'])
        if (d['matches_nih']):
            score = score + 100
        if (d['matches_fda']):
            score =score + 100
        if (d['matches_drug_trade_name']):
            score = score + 100
        if (d['matches_fda_ingredient']):
            score = score + 100
        if (d['nci_funded']):
            score = score + 100
        return float((score/8)/100)
    

    def loop_dataframes(self):
        output = []
        for i, row in self.df_1.iterrows():
            res = []
            for j, jrow in self.df_2.iterrows():
                values = dict.fromkeys(['matches_date_range','title_value', 'matches_nih', 'matches_fda', 'matches_drug', 'matches_device', 'matches_radiation', 'matches_dietary', 'matches_biological', 'matches_genetic', 'matches_other', 'matches_drug_trade_name', 'matches_fda_ingredient', 'nci_funded'], True)
                values['matches_date_range'] = self.matches_date_range(row.START_DATE, row.COMPLETION_DATE, row.PRIMARY_COMPLETION_DATE, jrow.Filing_Date, jrow.Grant_or_Publication_Date)
                values['title_value'] = self.official_title_matches_patent_title(row.OFFICIAL_TITLE, jrow.Patent_Title)
                values['matches_nih'] = self.source_matches_nih_org(row.SOURCE, jrow.NIH_Grant_Recipient_Organization)
                values['matches_fda'] = self.source_matches_fda_applicant(row.SOURCE, jrow.FDA_Applicant)
                values['matches_drug'] = self.is_drug_inventive(row.INTERVENTION_TYPE, jrow.Drugs_and_Chemistry)
                values['matches_device'] = self.is_device_inventive(row.INTERVENTION_TYPE, jrow.Diagnostic_and_Surgical_Devices)
                values['matches_radiation'] = self.is_radiation_inventive(row.INTERVENTION_TYPE, jrow.Radiation_Measurement)
                values['matches_dietary'] = self.is_food_and_nutrition_inventive(row.INTERVENTION_TYPE, jrow.Food_and_Nutrition)
                values['matches_biological'] = self.is_cell_inventive(row.INTERVENTION_TYPE, jrow.Cells_and_Enzymes)
                values['matches_genetic'] = self.is_dna_inventive(row.INTERVENTION_TYPE, jrow.DNA_RNA_or_Protein_Sequence)
                values['matches_other'] = self.is_other_inventive(row.INTERVENTION_TYPE, jrow.Other_and_Preclassification)
                values['matches_drug_trade_name'] = self.contains_mesh_term_in_fda_drug_trade_name(row.MESH_TERM, jrow.FDA_Drug_Trade_Name)
                values['matches_fda_ingredient'] = self.contains_mesh_term_in_fda_ingredient(row.MESH_TERM, jrow.FDA_Ingredient)
                values['nci_funded'] = self.is_nci_funded(row.RECIEVED_NCI_FUNDING, jrow.NIH_Federal_Grant_Number)
                r = self.calc_results(values)
                res.append(r)
            nar = np.array(res)
            nar_max = np.amax(nar)
            itemindex = np.where(nar==nar_max)
            print('Row Complete: '.format(float(nar_max)))
            best_match = {'id': self.df_2.iloc[itemindex].Patent_or_Publication_ID, 'value': float(nar_max)}
            output.append(best_match)
        return output


