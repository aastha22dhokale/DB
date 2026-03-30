LAST 4 DAYS IM DOING SAME TASK PLZ HELP ME WITH FINAL VERSION 
package com.ing.datadist.api.service;

import com.ing.datadist.api.model.*;
import com.ing.datadist.api.utils.OnePamDateUtil;
import com.ing.datadist.api.utils.RegkeyEnum;
import com.ing.datadist.dao.ApiTransactionLogDAO;
import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.dao.IncapablesAccountingDAO;
import com.ing.datadist.dao.IncapablesDAO.IncapableDbRecord;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import com.ing.datadist.enums.ErrorType;
import com.ing.datadist.enums.IncapablesObjectCode;
import com.ing.datadist.enums.RecordType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.ing.datadist.api.utils.DataDistributionConstants.*;

/**
 * Service for processing Incapables API calls.
 * Implements business logic for all Object Codes.
 * Errors are logged to error log table - no status updates to DD_INCAPABLES_TBL.
 */
@Service
@Slf4j
public class IncapablesProcessingService {
    private static final String LEGAL_COMPETENCY_INCAPABLE = "LGL_INCMP_ADLT";
    private static final String LEGAL_COMPETENCY_CAPABLE = "LGL_CMP_ADLT";
    private static final String INDIVIDUAL_LIFECYCLE_ACTIVE = "LIVING_IDV";
    private static final String ING_ENTITY = "1079";
    private static final String INDIVIDUAL_NAME_TYPE = "FORML_NM";
    private static final String FLOW_NAME = "INCAPABLES";

    private static final DateTimeFormatter OUTPUT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private final IdentifyInvolvedPartySearchService identifyService;
    private final InvolvedPartyOnePamRepository involvedPartyRepository;
    private final IndividualOnePamRepository individualRepository;
    private final AssociatedPartyRelationshipService relationshipService;
    private final ErrorLogDAO errorLogDAO;
    private final ApiTransactionLogDAO apiTransactionLogDAO;
    private final IncapablesAccountingDAO incapablesAccountingDAO;
    private final InvolvedPartyOnePamRepository involvedPartyOnePamRepository;
    private final InvolvedPartySearchService involvedPartySearchService;
    private final AddressService addressService;
    private final UpdateIndividualsService updateIndividualsService;
    private final PartyMarkingService partyMarkingService;

    public IncapablesProcessingService(
            IdentifyInvolvedPartySearchService identifyService,
            InvolvedPartyOnePamRepository involvedPartyRepository,
            IndividualOnePamRepository individualRepository,
            AssociatedPartyRelationshipService relationshipService,
            ErrorLogDAO errorLogDAO,
            ApiTransactionLogDAO apiTransactionLogDAO,
            IncapablesAccountingDAO incapablesAccountingDAO,
            InvolvedPartyOnePamRepository involvedPartyOnePamRepository,
            InvolvedPartySearchService involvedPartySearchService,
            AddressService addressService,
            PartyMarkingService partyMarkingService,
            UpdateIndividualsService updateIndividualsService) {
        this.identifyService = identifyService;
        this.involvedPartyRepository = involvedPartyRepository;
        this.individualRepository = individualRepository;
        this.relationshipService = relationshipService;
        this.errorLogDAO = errorLogDAO;
        this.apiTransactionLogDAO = apiTransactionLogDAO;
        this.incapablesAccountingDAO = incapablesAccountingDAO;
        this.involvedPartyOnePamRepository = involvedPartyOnePamRepository;
        this.involvedPartySearchService = involvedPartySearchService;
        this.addressService = addressService;
        this.updateIndividualsService = updateIndividualsService;
        this.partyMarkingService = partyMarkingService;
    }

    /**
     * Process a single incapable record based on object code.
     * Errors are logged to error log table - no status updates to DD_INCAPABLES_TBL.
     */
    public void processRecord(IncapableDbRecord record) {
        log.info("Data_Distributor_Incapables: Processing record DD_UUID={}, ObjectCode={}",
                record.getDdUuid(), record.getObjectCode());

        try {
            IncapablesObjectCode objectCode = IncapablesObjectCode.fromCode(record.getObjectCode());

            if (objectCode == null) {
                String errorMsg = "Unknown object code: " + record.getObjectCode();
                log.error("Data_Distributor_Incapables: {}", errorMsg);
                errorLogDAO.logValidationError(
                        RecordType.INCAPABLE,
                        record.getIncLastName(),
                        errorMsg,
                        record.getDdUuid(),
                        null,
                        record.getFileId()
                );
                return;
            }

            switch (objectCode) {
                case NEW_INABILITY_AND_RESPONSIBLE:
                    processObjectCode1001(record);
                    break;
                case CHANGE_OF_RESPONSIBLE:
                    processObjectCode1002(record);
                    break;
                case LIFTING_ADMINISTRATION:
                    processObjectCode1003(record);
                    break;
                case NEW_PRESCRIPTION:
                    processObjectCode1004(record);
                    break;
                default:
                    log.error("Data_Distributor_Incapables: Unhandled object code: {}", record.getObjectCode());
            }

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error processing record DD_UUID={}: {}",
                    record.getDdUuid(), e.getMessage(), e);
            errorLogDAO.logError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    ErrorType.PROCESSING_ERROR,
                    e.getMessage(),
                    null,
                    null,
                    getStackTrace(e),
                    record.getFileId()
            );
        }
    }

    /**
     * Object Code 1001: New Inability & Responsible
     */
    private void processObjectCode1001(IncapableDbRecord record) {
        incapablesAccountingDAO.insertSkeletonRecord(record);

        log.info("Data_Distributor_Incapables: Processing Object Code 1001 for DD_UUID={}", record.getDdUuid());

        String incOnePamUuid = searchOrCreateIncapablePerson(record);
        boolean searchReturnedNull = (incOnePamUuid == null);
        if (!searchReturnedNull) {
            SearchIndividualResponseV1 searchSnap =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());
            if (!searchReturnedNull) {
                incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                        record.getDdUuid(),
                        searchSnap,
                        record.getFileId()
                );
            }

        }

        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);
        if (incOnePamUuid == null) {
            errorLogDAO.logError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    ErrorType.API_ERROR,
                    "Failed to get/create INC party",
                    null, null, null,
                    record.getFileId()
            );
            return;
        }
        try {
            IncapableRelationship wrapper = buildRelationshipWrapper(record);
            SearchIndividualResponseV1 incDetails =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

            updateIndividualsService.reconcileCoreFields(UpdateIndividualsService.PartyRole.INCAPABLE, wrapper, incDetails);
            updateIndividualsService.reconcilePostalAddress(UpdateIndividualsService.PartyRole.INCAPABLE, wrapper, incDetails);
            SearchIndividualResponseV1 incUpdated =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

        } catch (Exception e) {
            log.error("INC update flow failed for UUID={}", incOnePamUuid, e);
        }

        String admOnePamUuid = searchOrCreateAdministrator(record);
        if (admOnePamUuid == null) {
            errorLogDAO.logError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    ErrorType.API_ERROR,
                    "Failed to get/create ADM party",
                    null, null, null,
                    record.getFileId()
            );
            return;
        }

        try {
            IncapableRelationship wrapper = buildRelationshipWrapper(record);
            SearchIndividualResponseV1 admDetails =
                    involvedPartySearchService.getIndividualByUuid(admOnePamUuid, record.getFileId());

            updateIndividualsService.reconcileCoreFields(UpdateIndividualsService.PartyRole.ADMINISTRATOR, wrapper, admDetails);
            updateIndividualsService.reconcilePostalAddress(UpdateIndividualsService.PartyRole.ADMINISTRATOR, wrapper, admDetails);
            incapablesAccountingDAO.updateAdministratorOnePamSnapshot(
                    record.getDdUuid(),
                    admDetails,
                    record.getFileId()
            );
        } catch (Exception e) {
            log.error("ADM update flow failed for UUID={}", admOnePamUuid, e);
        }

        String relationshipUuid = createOrUpdateRelationship(record, incOnePamUuid, admOnePamUuid);
        if (relationshipUuid != null) {
            log.info("Data_Distributor_Incapables: Relationship UUID={} for DD_UUID={}", relationshipUuid, record.getDdUuid());
        }


        try {
            log.info("Creating INCAPABLE party marking for INC UUID={}", incOnePamUuid);

            CreatePartyMarkingRequest pmReq = CreatePartyMarkingRequest.builder()
                    .involvedPartyMarkingType("ADLT_U_LGL_PROT")
                    .effectiveDate(OnePamDateUtil.toOnePamDate(record.getInabilityEffectiveDate()))
                    .endDate(OnePamDateUtil.toOnePamDate(record.getInabilityEndDate()))
                    .build();

            CreatePartyMarkingResponse partyMarkingResponse =
                    partyMarkingService.addPartyMarking(incOnePamUuid, pmReq);

            incapablesAccountingDAO.updateMarkingOnePamSnapshot(
                    record.getDdUuid(),
                    partyMarkingResponse.getInvolvedPartyMarking().getInvolvedPartyMarkingType(),
                    partyMarkingResponse.getInvolvedPartyMarking().getEffectiveDate(),
                    record.getObjectCode()
            );

            log.info("INC PartyMarking created successfully, markingId={}");
            apiTransactionLogDAO.logApiSuccess(
                    record.getDdUuid(),
                    record.getFileId(),
                    FLOW_NAME,
                    RegkeyEnum.CREATE_PARTY_MARKINGS.endpoint,
                    RegkeyEnum.CREATE_PARTY_MARKINGS.method,
                    200
            );


        } catch (Exception ex) {
            log.error("Failed to create INCAPABLE PartyMarking for DD_UUID={}: {}",
                    record.getDdUuid(), ex.getMessage(), ex);

            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "PartyMarking creation failed: " + ex.getMessage(),
                    "/v5/involved-parties/{uuid}/party-markings",
                    null, null, null, null,
                    record.getFileId());
        }
        log.info("Data_Distributor_Incapables: Successfully processed Object Code 1001 for DD_UUID={}", record.getDdUuid());
    }

    private void processObjectCode1002(IncapableDbRecord record) {
        log.info("Data_Distributor_Incapables: Processing Object Code 1002 (Change of Responsible) for DD_UUID={}",
                record.getDdUuid());

        // Step 1: Search Incapacitated Person - MUST exist for change of responsible
        String incOnePamUuid = searchIncapablePerson(record);
        if (incOnePamUuid == null) {
            String errorMsg = "INC party not found for Change of Responsible - incapable MUST exist. DD_UUID=" + record.getDdUuid();
            log.error("Data_Distributor_Incapables: {}", errorMsg);
            errorLogDAO.logValidationError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    errorMsg,
                    record.getDdUuid(),
                    null,
                    record.getFileId()
            );
            return;
        } else {
            incapablesAccountingDAO.insertSkeletonRecord(record);

            SearchIndividualResponseV1 searchResp =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

            incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                    record.getDdUuid(), searchResp, record.getFileId()
            );

            updateIncapablePerson(record, incOnePamUuid);

            try {
                IncapableRelationship wrapper = buildRelationshipWrapper(record);

                SearchIndividualResponseV1 incDetails =
                        involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

                updateIndividualsService.reconcileCoreFields(
                        UpdateIndividualsService.PartyRole.INCAPABLE,
                        wrapper,
                        incDetails
                );

                updateIndividualsService.reconcilePostalAddress(
                        UpdateIndividualsService.PartyRole.INCAPABLE,
                        wrapper,
                        incDetails
                );
                // fetch latest OnePAM snapshot
                SearchIndividualResponseV1 incUpdated =
                        involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

/*
                incapablesAccountingDAO.updateIncapableOnePamSnapshot(record.getDdUuid(), incUpdated, record.getFileId());
*/
                incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

            } catch (Exception e) {
                log.error("INC update flow failed for UUID={}", incOnePamUuid, e);
            }
        }

        // Step 2: Search/Create new Administrator
        String admOnePamUuid = searchOrCreateAdministrator(record);
        if (admOnePamUuid == null) {
            log.error("Data_Distributor_Incapables: Failed to get/create ADM party for DD_UUID={}", record.getDdUuid());
            errorLogDAO.logError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    ErrorType.API_ERROR,
                    "Failed to create ADM party",
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return;
        }

        // Step 3: Close existing relationships
        closeExistingRelationships(record, incOnePamUuid);

        // Step 4: Create new relationship with new Administrator
        String relationshipUuid = createNewRelationship(record, incOnePamUuid, admOnePamUuid);
        if (relationshipUuid != null) {
            log.info("Data_Distributor_Incapables: Relationship UUID={} created for DD_UUID={}",
                    relationshipUuid, record.getDdUuid());

        }

        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

        log.info("Data_Distributor_Incapables: Successfully processed Object Code 1002 for DD_UUID={}",
                record.getDdUuid());
    }


    
    private void processObjectCode1003(IncapableDbRecord record) {

        log.info("Processing Object Code 1003 for DD_UUID={}", record.getDdUuid());

        // Step 1: INC must exist

        String incOnePamUuid = searchIncapablePerson(record);

        if (incOnePamUuid == null) {

            errorLogDAO.logValidationError(

                    RecordType.INCAPABLE,

                    record.getIncLastName(),

                    "INC party must exist for lifting administration",

                    record.getDdUuid(),

                    null,

                    record.getFileId()

            );

            return;

        }

        // Step 2: ADM must exist

        String admOnePamUuid = searchAdministrator(record);

        if (admOnePamUuid == null) {

            errorLogDAO.logValidationError(

                    RecordType.ADMINISTRATOR,

                    record.getAdmLastName(),

                    "Administrator must exist for lifting administration",

                    record.getDdUuid(),

                    null,

                    record.getFileId()

            );

            return;

        }

        incapablesAccountingDAO.insertSkeletonRecord(record);

        // Step 3: Update INC to CAPABLE

        updateIncapablePersonToCapable(record, incOnePamUuid);

        // Step 4: Update ADMIN

        updateAdministrator(record, admOnePamUuid);

        // Step 5: Close relationship

        String closedRelationshipUuid =

                closeExistingRelationships(record, incOnePamUuid);

        if (closedRelationshipUuid == null) {

            errorLogDAO.logValidationError(

                    RecordType.INCAPABLE_RELATIONSHIP,

                    record.getIncLastName(),

                    "Relationship must exist to end administration",

                    record.getDdUuid(),

                    null,

                    record.getFileId()

            );

            return;

        }

        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

        log.info("Successfully processed Object Code 1003 for DD_UUID={}",

                record.getDdUuid());

    }

    /**
     * Object Code 1003: Lifting Administration (Capable)
     *//*
    private void processObjectCode1003(IncapableDbRecord record) {
        log.info("Data_Distributor_Incapables: Processing Object Code 1003 (Lifting Administration) for DD_UUID={}",
                record.getDdUuid());

        String incOnePamUuid = searchIncapablePerson(record);
        if (incOnePamUuid == null) {
            String errorMsg = "INC party not found for Lifting Administration - incapable MUST exist. DD_UUID=" + record.getDdUuid();
            log.error("Data_Distributor_Incapables: {}", errorMsg);
            errorLogDAO.logValidationError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    errorMsg,
                    record.getDdUuid(),
                    null,
                    record.getFileId()
            );
            return;
        }

        // Step 2: Search Administrator - MUST exist
        String admOnePamUuid = searchAdministrator(record);
        if (admOnePamUuid == null) {
            String errorMsg = "Administrator not found for lifting administration - DD_UUID=" + record.getDdUuid();
            log.error("Data_Distributor_Incapables: {}", errorMsg);
            errorLogDAO.logValidationError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    errorMsg,
                    record.getDdUuid(),
                    null,
                    record.getFileId()
            );
            return;
        } else {
            incapablesAccountingDAO.insertSkeletonRecord(record);

            SearchIndividualResponseV1 searchResp =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

            incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                    record.getDdUuid(), searchResp, record.getFileId()
            );

            updateAdministrator(record, admOnePamUuid);

            try {
                IncapableRelationship wrapper = buildRelationshipWrapper(record);

                SearchIndividualResponseV1 admDetails =
                        involvedPartySearchService.getIndividualByUuid(admOnePamUuid, record.getFileId());

                updateIndividualsService.reconcileCoreFields(
                        UpdateIndividualsService.PartyRole.ADMINISTRATOR,
                        wrapper,
                        admDetails
                );

                updateIndividualsService.reconcilePostalAddress(
                        UpdateIndividualsService.PartyRole.ADMINISTRATOR,
                        wrapper,
                        admDetails
                );

                SearchIndividualResponseV1 admUpdated =
                        involvedPartySearchService.getIndividualByUuid(admOnePamUuid, record.getFileId());

                incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

            } catch (Exception e) {
                log.error("ADM update flow failed for UUID={}", admOnePamUuid, e);
            }
        }

        updateIncapablePersonToCapable(record, incOnePamUuid);

        try {
            SearchIndividualResponseV1 incUpdated =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

*//*
            incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                    record.getDdUuid(), incUpdated, record.getFileId());
*//*
        }
        catch (Exception ex){
            log.error("INC snapshot update failed for 1003", ex);
        }


        // Step 4: Close existing relationship
        String closedRelationshipUuid = closeExistingRelationships(record, incOnePamUuid);
        if (closedRelationshipUuid == null) {
            String errorMsg = "Relationship not found - cannot end administration for DD_UUID=" + record.getDdUuid();
            log.error("Data_Distributor_Incapables: {}", errorMsg);
            errorLogDAO.logValidationError(
                    RecordType.INCAPABLE_RELATIONSHIP,
                    record.getIncLastName(),
                    errorMsg,
                    record.getDdUuid(),
                    null,
                    record.getFileId()
            );
        }
        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

        log.info("Data_Distributor_Incapables: Successfully processed Object Code 1003 for DD_UUID={}",
                record.getDdUuid());
    }*/

    /**
     * Object Code 1004: New Prescription
     */
    private void processObjectCode1004(IncapableDbRecord record) {
        incapablesAccountingDAO.insertSkeletonRecord(record);
        log.info("Data_Distributor_Incapables: Processing Object Code 1004 (New Prescription) for DD_UUID={}",
                record.getDdUuid());

        String incOnePamUuid = searchOrCreateIncapablePerson(record);

        boolean searchReturnedNull = (incOnePamUuid == null);

        if (!searchReturnedNull) {
            SearchIndividualResponseV1 snap =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

            if (snap != null && snap.getIndividualNames() != null && !snap.getIndividualNames().isEmpty()) {
                incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                        record.getDdUuid(), snap, record.getFileId());
            }
        }

        if (incOnePamUuid == null) {
            log.warn("Data_Distributor_Incapables: INC party not found for new prescription - creating new (unexpected) for DD_UUID={}",
                      record.getDdUuid());
            incOnePamUuid = createIncapablePerson(record);
            incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);
            if (incOnePamUuid == null) {
                errorLogDAO.logError(
                        RecordType.INCAPABLE,
                        record.getIncLastName(),
                        record.getDdUuid(),
                        ErrorType.API_ERROR,
                        "Failed to create INC party",
                        null,
                        null,
                        null,
                        record.getFileId()
                );
                return;
            }
        } else {
            // Update existing INC party
            SearchIndividualResponseV1 searchResp =
                    involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());
/*
            incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                    record.getDdUuid(), searchResp, record.getFileId()
            );*/

            updateIncapablePerson(record, incOnePamUuid);
            // ---- UPDATE INC ----
            try {
                IncapableRelationship wrapper = buildRelationshipWrapper(record);

                SearchIndividualResponseV1 incDetails =
                        involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());

                updateIndividualsService.reconcileCoreFields(
                        UpdateIndividualsService.PartyRole.INCAPABLE,
                        wrapper,
                        incDetails
                );

                updateIndividualsService.reconcilePostalAddress(
                        UpdateIndividualsService.PartyRole.INCAPABLE,
                        wrapper,
                        incDetails
                );
                SearchIndividualResponseV1 incUpdated =
                        involvedPartySearchService.getIndividualByUuid(incOnePamUuid, record.getFileId());
                incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

            } catch (Exception e) {
                log.error("INC update flow failed for UUID={}", incOnePamUuid, e);
            }
        }

        // Step 2: Search/Create Administrator
        String admOnePamUuid = searchOrCreateAdministrator(record);
        if (admOnePamUuid == null) {
            log.warn("Data_Distributor_Incapables: ADM party not found for new prescription - creating new (unexpected) for DD_UUID={}",
                    record.getDdUuid());
            admOnePamUuid = createAdministrator(record);
            if (admOnePamUuid == null) {
                errorLogDAO.logError(
                        RecordType.ADMINISTRATOR,
                        record.getAdmLastName(),
                        record.getDdUuid(),
                        ErrorType.API_ERROR,
                        "Failed to create ADM party",
                        null,
                        null,
                        null,
                        record.getFileId()
                );
                return;
            }
        } else {

            updateAdministrator(record, admOnePamUuid);

            try {
                IncapableRelationship wrapper = buildRelationshipWrapper(record);

                SearchIndividualResponseV1 admDetails =
                        involvedPartySearchService.getIndividualByUuid(admOnePamUuid, record.getFileId());

                incapablesAccountingDAO.updateAdministratorOnePamSnapshot(
                        record.getDdUuid(), admDetails, record.getFileId());

                updateIndividualsService.reconcileCoreFields(
                        UpdateIndividualsService.PartyRole.ADMINISTRATOR,
                        wrapper,
                        admDetails
                );

                updateIndividualsService.reconcilePostalAddress(
                        UpdateIndividualsService.PartyRole.ADMINISTRATOR,
                        wrapper,
                        admDetails
                );
                SearchIndividualResponseV1 admUpdated =
                        involvedPartySearchService.getIndividualByUuid(admOnePamUuid, record.getFileId());
/*

                incapablesAccountingDAO.updateAdministratorOnePamSnapshot(
                        record.getDdUuid(), admUpdated, record.getFileId());

*/        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);


            } catch (Exception e) {
                log.error("ADM update flow failed for UUID={}", admOnePamUuid, e);
            }
        }

        // Step 3: Update or create relationship with extended dates
        String relationshipUuid = updateOrCreateRelationshipWithExtension(record, incOnePamUuid ,admOnePamUuid );
        if (relationshipUuid != null) {
            log.info("Data_Distributor_Incapables: Relationship UUID={} extended for DD_UUID={}",
                    relationshipUuid, record.getDdUuid());
        }
        log.info("Data_Distributor_Incapables: Successfully processed Object Code 1004 for DD_UUID={}",
                record.getDdUuid());
        incapablesAccountingDAO.updateDiFields(record.getDdUuid(), record);

    }

    /**
     * Search for incapacitated person in OnePAM using identify API.
     * Apply perfect or no match rule based on: First & Last Name, DOB and Postal Code
     */
    /*
    private String searchOrCreateIncapablePerson(IncapableDbRecord record) {
        try {
            // Step 1: Search using identify API with postal address for enhanced matching
            String postalCode = (record.getIncPostalCode() != null && !record.getIncPostalCode().trim().isEmpty())
                    ? record.getIncPostalCode().trim()
                    : null;

            IdentifyInvolvedPartiesResponse searchResponse = identifyService.identifyIndividuals(
                    formatDateForApi(record.getIncDateOfBirth()),
                    INDIVIDUAL_NAME_TYPE,
                    record.getIncLastName(),
                    postalCode,
                    record.getDdUuid(),
                    record.getFileId()
            );

            if (searchResponse != null && searchResponse.getIndividuals() != null
                    && !searchResponse.getIndividuals().isEmpty()) {

                int matchCount = searchResponse.getIndividuals().size();

                if (matchCount == 1) {
                    // Exact match found - get OnePAM UUID and UPDATE the party
                    String onePamUuid = searchResponse.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("Data_Distributor_Incapables: INC party exact match found, UUID={}, proceeding to update", onePamUuid);

                    // Log API transaction for search
                    apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                            FLOW_NAME, RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.endpoint,
                            RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.method, 200);

                    // UPDATE existing INC party with latest data
                    updateIncapablePerson(record, onePamUuid);

                    return onePamUuid;
                } else {
                    // Potential Match - multiple parties found, requires manual review
                    String errorMsg = "Multiple INC parties found (Potential Match) - requires manual review. Count=" + matchCount;
                    log.warn("Data_Distributor_Incapables: {}", errorMsg);
                    errorLogDAO.logValidationError(
                            RecordType.INCAPABLE,
                            record.getIncLastName(),
                            errorMsg,
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null; // Skip record for manual review
                }
            }

            // Not found - create new individual
            log.info("Data_Distributor_Incapables: INC party not found (No Match), creating new individual for DD_UUID={}",
                    record.getDdUuid());
            return createIncapablePerson(record);

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error searching INC party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Search INC party failed: " + e.getMessage(),
                    RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }*/
    private String searchOrCreateIncapablePerson(IncapableDbRecord record) {

        // Stage 0: internal-id (CSI_BE == DD_UUID) – MOST RELIABLE
        String byInternalId = searchIncapableByInternalId(record.getDdUuid(), record.getFileId());
        if (byInternalId != null) {
            updateIncapablePerson(record, byInternalId);
            return byInternalId;
        }

        try {
            String dob = formatDateForApi(record.getIncDateOfBirth());
            String postalCode =
                    (record.getIncPostalCode() != null && !record.getIncPostalCode().isBlank())
                            ? record.getIncPostalCode().trim()
                            : null;

            IdentifyInvolvedPartiesResponse searchResponse =
                    identifyService.identifyIndividuals(
                            dob,
                            INDIVIDUAL_NAME_TYPE,
                            record.getIncLastName(),
                            postalCode,
                            record.getDdUuid(),
                            record.getFileId()
                    );

            if (searchResponse != null &&
                    searchResponse.getIndividuals() != null &&
                    !searchResponse.getIndividuals().isEmpty()) {

                int matchCount = searchResponse.getIndividuals().size();

                if (matchCount == 1) {
                    String onePamUuid =
                            searchResponse.getIndividuals().get(0).getInvolvedPartyIdentifier();

                    log.info("INC Identify exact match, UUID={}, updating", onePamUuid);
                    updateIncapablePerson(record, onePamUuid);
                    return onePamUuid;
                }

                errorLogDAO.logValidationError(
                        RecordType.INCAPABLE,
                        record.getIncLastName(),
                        "Multiple INC matches found: " + matchCount,
                        record.getDdUuid(),
                        null,
                        record.getFileId()
                );
                return null;
            }

        } catch (Exception e) {
            log.error("Identify INC failed: {}", e.getMessage());
        }

        // Stage 2: Create if not found
        return createIncapablePerson(record);


    }


    /**
     * Update existing incapacitated person in OnePAM
     */
    private void updateIncapablePerson(IncapableDbRecord record, String onePamUuid) {
        try {
            UpdateInvolvedPartyIndividualRequestV5 individual = UpdateInvolvedPartyIndividualRequestV5.builder()
                    .dataSource(DATA_SOURCE)
                    .countryOfResidence(mapCountryCode(record.getIncCountryOfResidence()))
                    .effectiveDate(getCurrentDateISO())
                    .legalCompetencyStatusType(LEGAL_COMPETENCY_INCAPABLE)
                    .individualLifeCycleStatusType(INDIVIDUAL_LIFECYCLE_ACTIVE)
                    .cityOfBirth(record.getIncCityOfBirth())
                    .countryOfBirth(mapCountryCode(record.getIncCountryOfBirth()))
                    .dateOfBirth(record.getIncDateOfBirth())
                    .dateOfDeath(record.getIncDateOfDeath())
                    .gender(record.getIncGender())
                    .build();

            PatchIndividualRequest request = PatchIndividualRequest.builder()
                    .individual(individual)
                    .build();

            log.info("Data_Distributor_Incapables: Updating INC party OnePAM UUID={}", onePamUuid);
            individualRepository.updateIndividual(onePamUuid, request).get();

            log.info("Data_Distributor_Incapables: INC party updated successfully, OnePAM UUID={}", onePamUuid);

            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.UPDATE_INVOLVED_PARTY.endpoint,
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.method, 200);

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error updating INC party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Update INC party failed: " + e.getMessage(),
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.resolveEndpoint(onePamUuid),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            // Don't throw - continue processing with found UUID
        }
    }

    /**
     * Create a new incapacitated person in OnePAM
     */
    private String createIncapablePerson(IncapableDbRecord record) {
        try {
            CreateInternalIdentifierRequest internalId = CreateInternalIdentifierRequest.builder()
                    .involvedPartyInternalIdentifierType("CSI_BE")
                    .involvedPartyInternalIdentifierValue(record.getDdUuid())
                    .build();

            IndividualNameRequest name = IndividualNameRequest.builder()
                    .lastName(record.getIncLastName())
                    .firstName1(record.getIncFirstName())
/*
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
*/
                    .build();
            CreateIndividualRequest individual = CreateIndividualRequest.builder()
                    .dataSource(DATA_SOURCE)
                    .logicalDataDomain(LOGICAL_DATA_DOMAIN)
                    .countryOfResidence(mapCountryCode(record.getIncCountryOfResidence()))
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
                    .legalCompetencyStatusType(LEGAL_COMPETENCY_INCAPABLE)
                    .individualLifeCycleStatusType(INDIVIDUAL_LIFECYCLE_ACTIVE)
                    .channelOfEntry(CHANNEL_OF_ENTRY)
                    .preferredLanguage(PREFERRED_LANGUAGE)
                    .cityOfBirth(record.getIncCityOfBirth())
                    .countryOfBirth(mapCountryCode(record.getIncCountryOfBirth()))
                    .dateOfBirth(record.getIncDateOfBirth())
                    .dateOfDeath(record.getIncDateOfDeath())
                    .dateOfBirthQualityIndicator("COMPL")
                    .gender(record.getIncGender())
                    .individualName(name)
                    .involvedPartyInternalIdentifiers(Collections.singletonList(internalId))
                    .build();

            CreateInvolvedPartyRequestV5 request = CreateInvolvedPartyRequestV5.builder()
                    .individual(individual)
                    .build();

            log.info("Data_Distributor_Incapables: Creating INC party for DD_UUID={}", record.getDdUuid());
            CreateInvolvedPartyResponseV5 response = involvedPartyRepository.createInvolvedParty(request).get();



            SearchIndividualResponseV1 incapableResponse = convert(response);
/*
            incapablesAccountingDAO.updateIncapableOnePamSnapshot(
                    record.getDdUuid(),
                    incapableResponse,
                    record.getFileId()
            );
*/

            if (response == null || response.getIndividual() == null) {
                log.error("Create Admin failed: response body missing 'individual' (likely 4xx). DD_UUID={}, ADM lastName={}",
                        record.getDdUuid(), record.getAdmLastName());
                return null;
            }

            String onePamUuid = response.getIndividual().getInvolvedPartyIdentifier();
            log.info("Data_Distributor_Incapables: INC party created, OnePAM UUID={}", onePamUuid);
            if (hasPostalAddress(record.getIncStreetName(), record.getIncPostalCode())) {
                CreatePostalAddressRequest pa = CreatePostalAddressRequest.builder()
                        .postalAddressUsageType("RSDNT_ADR")
                        .streetName(record.getIncStreetName())
                        .houseNumber(record.getIncHouseNumber())
                        .houseNumberAddition(record.getIncHouseNumberAddition())
                        .postalCode(record.getIncPostalCode())
                        .cityName(record.getIncCityName())
                        .countryCode(mapCountryCode(record.getIncCountryOfResidence()))
                        .dataSource(DATA_SOURCE)
                        .build();
/*
                addressService.createPostalAddress(onePamUuid, pa, record.getFileId());
*/

                try {
                    PostalAddressResponse paResp =
                            addressService.createPostalAddress(onePamUuid, pa, record.getFileId());


                    SearchIndividualResponseV1 snap = convert(response, paResp);
/*
                    incapablesAccountingDAO.updateIncapableOnePamSnapshot(record.getDdUuid(), snap, record.getFileId());
*/


                    apiTransactionLogDAO.logApiSuccess(
                            record.getDdUuid(),
                            record.getFileId(),
                            FLOW_NAME,
                            RegkeyEnum.CREATE_POSTAL_ADDRESS.endpoint,
                            RegkeyEnum.CREATE_POSTAL_ADDRESS.method,
                            201
                    );
                }
                catch (Exception ex) {

                    errorLogDAO.logApiError(
                            RecordType.ADMINISTRATOR,
                            record.getIncLastName(),
                            record.getDdUuid(),
                            null,
                            "Postal Address creation failed: " + ex.getMessage(),
                            RegkeyEnum.CREATE_POSTAL_ADDRESS.resolveEndpoint(onePamUuid),
                            null,
                            null,
                            null,
                            null,
                            record.getFileId()
                    );

                }

            }
            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.CREATE_INVOLVED_PARTY.endpoint,
                    RegkeyEnum.CREATE_INVOLVED_PARTY.method, 200);

            return onePamUuid;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error creating INC party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Create INC party failed: " + e.getMessage(),
                    RegkeyEnum.CREATE_INVOLVED_PARTY.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    /**
     * Search for administrator in OnePAM using identify API.
     * If found (Exact Match), update the existing party.
     * If not found (No Match), create a new individual.
     * If multiple found (Potential Match), skip for manual review and log error.
     */
    /*
    private String searchOrCreateAdministrator(IncapableDbRecord record) {
        try {
            // Build search request for administrator
            String admPostalCode = (record.getAdmPostalCode() != null && !record.getAdmPostalCode().trim().isEmpty())
                    ? record.getAdmPostalCode().trim()
                    : null;

            IdentifyInvolvedPartiesResponse searchResponse = identifyService.identifyIndividuals(
                    null,  // ADM may not have date of birth
                    INDIVIDUAL_NAME_TYPE,
                    record.getAdmLastName(),
                    admPostalCode,
                    record.getDdUuid(),
                    record.getFileId()
            );


            if (searchResponse != null && searchResponse.getIndividuals() != null
                    && !searchResponse.getIndividuals().isEmpty()) {

                int matchCount = searchResponse.getIndividuals().size();

                if (matchCount == 1) {
                    // Exact match found - get OnePAM UUID and UPDATE the party
                    String onePamUuid = searchResponse.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("Data_Distributor_Incapables: ADM party exact match found, UUID={}, proceeding to update", onePamUuid);

                    // Log API transaction for search
                    apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                            FLOW_NAME, RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.endpoint,
                            RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.method, 200);

                    // UPDATE existing ADM party with latest data
                    updateAdministrator(record, onePamUuid);

                    return onePamUuid;
                } else {
                    // Potential Match - multiple parties found, requires manual review
                    String errorMsg = "Multiple ADM parties found (Potential Match) - requires manual review. Count=" + matchCount;
                    log.warn("Data_Distributor_Incapables: {}", errorMsg);
                    errorLogDAO.logValidationError(
                            RecordType.ADMINISTRATOR,
                            record.getAdmLastName(),
                            errorMsg,
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null; // Skip record for manual review
                }
            }

            // Not found - create new administrator
            log.info("Data_Distributor_Incapables: ADM party not found (No Match), creating new individual for DD_UUID={}",
                    record.getDdUuid());
            return createAdministrator(record);

        } catch (Exception e) {
            String errorMsg = "Error searching ADM party";

            // Check if it's a timeout error
            if (e.getMessage() != null && e.getMessage().contains("exceeded") && e.getMessage().contains("seconds")) {
                errorMsg = "API timeout searching ADM party - " + e.getMessage();
                log.error("Data_Distributor_Incapables: {} (LastName: {})", errorMsg, record.getAdmLastName(), e);
            } else {
                log.error("Data_Distributor_Incapables: {}: {}", errorMsg, e.getMessage(), e);
            }

            errorLogDAO.logApiError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    null,
                    errorMsg + ": " + e.getMessage(),
                    RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }*/


    private String searchOrCreateAdministrator(IncapableDbRecord record) {
        try {
            String adminDdUuid = record.getAdminDdUuid();

            SearchInvolvedPartiesResponseV1 byId =
                    involvedPartySearchService.searchIndividualByInternalId(adminDdUuid, record.getFileId());

            if (byId != null && byId.getIndividuals() != null && !byId.getIndividuals().isEmpty()) {
                if (byId.getIndividuals().size() == 1) {
                    String uuid = byId.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("ADMIN matched by adminDdUuid, UUID={}", uuid);

                    updateAdministrator(record, uuid);
                    return uuid;
                } else {
                    errorLogDAO.logValidationError(
                            RecordType.ADMINISTRATOR,
                            record.getAdmLastName(),
                            "Multiple ADMIN matches found for adminDdUuid=" + adminDdUuid,
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null;
                }
            }

            log.info("ADMIN not found : creating new admin using adminDdUuid={}", adminDdUuid);
            return createAdministrator(record);

        } catch (Exception e) {
            log.error("Admin search failed: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Update existing administrator in OnePAM
     */
    private void updateAdministrator(IncapableDbRecord record, String onePamUuid) {
        try {
            UpdateInvolvedPartyIndividualRequestV5 individual = UpdateInvolvedPartyIndividualRequestV5.builder()
                    .dataSource(DATA_SOURCE)
                    .countryOfResidence(mapCountryCode(record.getAdmCountryOfResidence()))
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
                    .individualLifeCycleStatusType(INDIVIDUAL_LIFECYCLE_ACTIVE)
                    .build();

            PatchIndividualRequest request = PatchIndividualRequest.builder()
                    .individual(individual)
                    .build();

            log.info("Data_Distributor_Incapables: Updating ADM party OnePAM UUID={}", onePamUuid);
            individualRepository.updateIndividual(onePamUuid, request).get();

            log.info("Data_Distributor_Incapables: ADM party updated successfully, OnePAM UUID={}", onePamUuid);

            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.UPDATE_INVOLVED_PARTY.endpoint,
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.method, 200);

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error updating ADM party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    null,
                    "Update ADM party failed: " + e.getMessage(),
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.resolveEndpoint(onePamUuid),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            // Don't throw - continue processing with found UUID
        }
    }

    /**
     * Create a new administrator in OnePAM
     */
    private String createAdministrator(IncapableDbRecord record) {
        try {
            CreateInternalIdentifierRequest internalId = CreateInternalIdentifierRequest.builder()
                    .involvedPartyInternalIdentifierType("CSI_BE")
                    .involvedPartyInternalIdentifierValue(record.getAdminDdUuid())
                    .build();

            IndividualNameRequest name = IndividualNameRequest.builder()
                    .lastName(record.getAdmLastName())
                    .firstName1(record.getAdmFirstName())
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
                    .build();


            CreateIndividualRequest individual = CreateIndividualRequest.builder()
                    .dataSource(DATA_SOURCE)
                    .logicalDataDomain(LOGICAL_DATA_DOMAIN)
                    .countryOfResidence(mapCountryCode(record.getAdmCountryOfResidence()))
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
                    .legalCompetencyStatusType(LEGAL_COMPETENCY_CAPABLE)
                    .individualLifeCycleStatusType(INDIVIDUAL_LIFECYCLE_ACTIVE)
                    .channelOfEntry(CHANNEL_OF_ENTRY)
                    .preferredLanguage(PREFERRED_LANGUAGE)
                    .individualName(name)
                    .involvedPartyInternalIdentifiers(Collections.singletonList(internalId))

                    .build();

            CreateInvolvedPartyRequestV5 request = CreateInvolvedPartyRequestV5.builder()
                    .individual(individual)
                    .build();

            log.info("Data_Distributor_Incapables: Creating ADM party for DD_UUID={}", record.getDdUuid());
            CreateInvolvedPartyResponseV5 response = involvedPartyRepository.createInvolvedParty(request).get();

            SearchIndividualResponseV1 snap = convert(response);
            // ✅ Save ADM OnePam Snapshot
/*
            incapablesAccountingDAO.updateAdministratorOnePamSnapshot(
                    record.getDdUuid(),
                    snap,
                    record.getFileId()
            );
*/
            if (response == null || response.getIndividual() == null) {
                log.error("Create Admin failed: response body missing 'individual' (likely 4xx). DD_UUID={}, ADM lastName={}",
                        record.getDdUuid(), record.getAdmLastName());
                return null; // don't dereference; let caller log error and continue
            }

            String onePamUuid = response.getIndividual().getInvolvedPartyIdentifier();

            log.info("Data_Distributor_Incapables: ADM party created, OnePAM UUID={}", onePamUuid);
            if (hasPostalAddress(record.getAdmStreetName(), record.getAdmPostalCode())) {
                CreatePostalAddressRequest postalAddrReq = CreatePostalAddressRequest.builder()
                        .postalAddressUsageType("RSDNT_ADR")
                        .streetName(record.getAdmStreetName())
                        .houseNumber(record.getAdmHouseNumber())
                        .houseNumberAddition(record.getAdmHouseNumberAddition())
                        .postalCode(record.getAdmPostalCode())
                        .cityName(record.getAdmCityName())
                        .countryCode(mapCountryCode(record.getAdmCountryOfResidence()))
                        .dataSource(DATA_SOURCE)
                        .build();

                PostalAddressResponse admPaResp =
                        addressService.createPostalAddress(onePamUuid, postalAddrReq, record.getFileId());


                SearchIndividualResponseV1 snap2 = convert(response, admPaResp);
/*
                incapablesAccountingDAO.updateAdministratorOnePamSnapshot(
                        record.getDdUuid(),
                        snap2,
                        record.getFileId()
                );*/
            }

            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.CREATE_INVOLVED_PARTY.endpoint,
                    RegkeyEnum.CREATE_INVOLVED_PARTY.method, 200);

            return onePamUuid;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error creating ADM party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    null,
                    "Create ADM party failed: " + e.getMessage(),
                    RegkeyEnum.CREATE_INVOLVED_PARTY.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    private String createOrUpdateRelationship(IncapableDbRecord record, String incOnePamUuid, String admOnePamUuid) {
        try {
            // Step 1: Search for existing relationship
            String existingRelationshipUuid = searchExistingRelationship(record, incOnePamUuid, admOnePamUuid);

            if (existingRelationshipUuid != null) {
                // Relationship exists - update it
                log.info("Data_Distributor_Incapables: Existing relationship found, UUID={}", existingRelationshipUuid);
                return existingRelationshipUuid; // For 1001, just return existing - update not needed
            }

            // Step 2: No existing relationship - create new one
            String effectiveDate = formatDateForApi(record.getInabilityEffectiveDate());
            if (effectiveDate == null) {
                effectiveDate = OnePamDateUtil.toOnePamDate(null);
            } else {
                effectiveDate = OnePamDateUtil.toOnePamDate(effectiveDate);
            }

            // Prepare end date
            String endDate = formatDateForApi(record.getInabilityEndDate());
            if (endDate != null) {
                endDate = OnePamDateUtil.toOnePamDate(endDate);
            }

            CreateAssociatedPartyRelationshipRequest.Grantor grantor =
                    CreateAssociatedPartyRelationshipRequest.Grantor.builder()
                            .id(incOnePamUuid)
/*
                            .effectiveDate(OnePamDateUtil.toOnePamDate(effectiveDate))
*/
                            .build();

            // Determine operational lifecycle status based on end date
            String operationalStatus = getOperationalLifeCycleStatus(endDate);

            CreateAssociatedPartyRelationshipRequest request = CreateAssociatedPartyRelationshipRequest.builder()
                    .relationshipType(GUARDIAN_RELATIONSHIP)
                    .lifeCycleStatusType(ASSOCIATED_PARTY_RELATIONSHIP_LIFE_CYCLE_STATUS)
                    .ingEntity(ING_ENTITY)
                    .grantee(CreateAssociatedPartyRelationshipRequest.Grantee.builder()
                            .id(admOnePamUuid)
                            .build())
                    .grantors(Collections.singletonList(grantor))
                    .effectiveDate(effectiveDate)
                    .endDate(endDate)
/*
                    .mixableIndicatorType("N")
*/
                    .operationalLifeCycleStatusType(operationalStatus)
                    .dataSource(DATA_SOURCE)
                    .logicalDataDomain(LOGICAL_DATA_DOMAIN)
                    .build();

            log.info("Data_Distributor_Incapables: Creating relationship INC={} -> ADM={}, Status={}",
                    incOnePamUuid, admOnePamUuid, operationalStatus);

            CreateAssociatedPartyRelationshipResponse response =
                    relationshipService.createAssociatedPartyRelationship(request, false);
/*
            incapablesAccountingDAO.updateRelationshipOnePamSnapshot(
                    record.getDdUuid(),
                    response.getAssociatedPartyRelationship(),
                    record.getFileId()
            );
*/
            String relationshipUuid = response.getAssociatedPartyRelationship().getRelationshipIdentifier();
            log.info("Data_Distributor_Incapables: Relationship created, UUID={}", relationshipUuid);

            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.endpoint,
                    RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.method, 200);

            return relationshipUuid;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error creating relationship: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE_RELATIONSHIP,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Create relationship failed: " + e.getMessage(),
                    RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    /**
     * Search for existing relationship between INC and ADM parties
     */
    private String searchExistingRelationship(IncapableDbRecord record, String incOnePamUuid, String admOnePamUuid) {
        try {
            log.info("Data_Distributor_Incapables: Searching existing relationship for INC={}", incOnePamUuid);

            // Get grantees for the INC party (grantor)
            GetGranteesResponse response = identifyService.getGrantees(incOnePamUuid, record.getDdUuid(), record.getFileId());

            if (response != null && response.getInvolvedPartyInvolvedPartyRelationships() != null
                    && response.getInvolvedPartyInvolvedPartyRelationships().getAssociatedPartyRelationships() != null) {
                for (AssociatedPartyRelationship relationship :
                        response.getInvolvedPartyInvolvedPartyRelationships().getAssociatedPartyRelationships()) {
                    // Check if the relationship is with the ADM party
                    log.info("Data_Distributor_Incapables: Found relationship UUID={}",
                            relationship.getRelationshipIdentifier());

                    // Log API transaction
                    apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                            FLOW_NAME, RegkeyEnum.GET_GRANTEES.endpoint,
                            RegkeyEnum.GET_GRANTEES.method, 200);

                    return relationship.getRelationshipIdentifier();
                }
            }

            log.info("Data_Distributor_Incapables: No existing relationship found for INC={}", incOnePamUuid);
            return null;

        } catch (Exception e) {
            log.warn("Data_Distributor_Incapables: Error searching existing relationship: {}, will create new",
                    e.getMessage());
            return null; // Return null to proceed with creation
        }
    }

    // ===================== Helper Methods =====================

    /**
     * Search for incapacitated person (INC) without creating - used for 1002, 1003, 1004
     */
    private String searchIncapablePerson(IncapableDbRecord record) {
        try {
            IdentifyInvolvedPartiesResponse searchResponse = identifyService.identifyIndividuals(
                    formatDateForApi(record.getIncDateOfBirth()),
                    INDIVIDUAL_NAME_TYPE,
                    record.getIncLastName(),
                    record.getIncPostalCode(),
                    record.getDdUuid(),
                    record.getFileId()
            );

            if (searchResponse != null && searchResponse.getIndividuals() != null) {
                int matchCount = searchResponse.getIndividuals().size();

                if (matchCount == 1) {
                    // Exact match
                    String onePamUuid = searchResponse.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("Data_Distributor_Incapables: INC party exact match found, UUID={}", onePamUuid);
                    apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                            FLOW_NAME, RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.endpoint,
                            RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.method, 200);
                    return onePamUuid;
                } else if (matchCount > 1) {
                    // Potential match - multiple parties found, requires manual review
                    String errorMsg = "Multiple INC parties found (Potential Match) - requires manual review. Count=" + matchCount;
                    log.warn("Data_Distributor_Incapables: {}", errorMsg);
                    errorLogDAO.logValidationError(
                            RecordType.INCAPABLE,
                            record.getIncLastName(),
                            errorMsg,
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null; // Skip record for manual review
                }
            }

            // No match
            log.info("Data_Distributor_Incapables: INC party not found for DD_UUID={}", record.getDdUuid());
            return null;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error searching INC party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Search INC party failed: " + e.getMessage(),
                    RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    /**
     * Search for administrator (ADM) without creating - used for 1002, 1003, 1004
     */

    /*
    private String searchAdministrator(IncapableDbRecord record) {
        try {
            IdentifyInvolvedPartiesRequest.PostalAddress postalAddress = null;
            if (hasPostalAddress(null, record.getAdmPostalCode())) {
                postalAddress = IdentifyInvolvedPartiesRequest.PostalAddress.builder()
                        .postalCode(record.getAdmPostalCode())
                        .houseNumber(record.getAdmHouseNumber())
                        .build();
            }

            IdentifyInvolvedPartiesRequest searchRequest = IdentifyInvolvedPartiesRequest.builder()
                    .individual(IdentifyInvolvedPartiesRequest.Individual.builder()
                            .individualName(IdentifyInvolvedPartiesRequest.IndividualName.builder()
                                    .individualNameType(INDIVIDUAL_NAME_TYPE)
                                    .lastName(record.getAdmLastName())
                                    .build())
                            .postalAddress(postalAddress)
                            .build())
                    .build();

            IdentifyInvolvedPartiesResponse searchResponse = identifyService.identifyIndividuals(
                    searchRequest,
                    record.getDdUuid(),
                    record.getFileId()
            );

            if (searchResponse != null && searchResponse.getIndividuals() != null) {
                int matchCount = searchResponse.getIndividuals().size();

                if (matchCount == 1) {
                    // Exact match found - Administrator exists
                    String onePamUuid = searchResponse.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("Data_Distributor_Incapables: ADM party exact match found, UUID={}", onePamUuid);
                    apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                            FLOW_NAME, RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.endpoint,
                            RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.method, 200);
                    return onePamUuid;
                } else if (matchCount > 1) {
                    // Potential match - multiple parties found, requires manual review
                    String errorMsg = "Multiple ADM parties found (Potential Match) - requires manual review. Count=" + matchCount;
                    log.warn("Data_Distributor_Incapables: {}", errorMsg);
                    errorLogDAO.logValidationError(
                            RecordType.ADMINISTRATOR,
                            record.getAdmLastName(),
                            errorMsg,
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null; // Skip record for manual review
                }
            }

            // No match
            log.info("Data_Distributor_Incapables: ADM party not found for DD_UUID={}", record.getDdUuid());
            return null;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error searching ADM party: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.ADMINISTRATOR,
                    record.getAdmLastName(),
                    record.getDdUuid(),
                    null,
                    "Search ADM party failed: " + e.getMessage(),
                    RegkeyEnum.IDENTIFY_INVOLVED_PARTIES.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }*/
    private String searchAdministrator(IncapableDbRecord record) {
        try {
            // search using ADMIN internal id
            SearchInvolvedPartiesResponseV1 byId =
                    involvedPartySearchService.searchIndividualByInternalId(
                            record.getAdminDdUuid(),
                            record.getFileId()
                    );
            if (byId != null && byId.getIndividuals() != null && !byId.getIndividuals().isEmpty()) {
                if (byId.getIndividuals().size() == 1) {
                    String uuid = byId.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("ADMIN matched by adminDdUuid, UUID={}", uuid);
                    return uuid;
                } else {
                    errorLogDAO.logValidationError(
                            RecordType.ADMINISTRATOR,
                            record.getAdmLastName(),
                            "Multiple ADMIN matches found for adminDdUuid=" + record.getAdminDdUuid(),
                            record.getDdUuid(),
                            null,
                            record.getFileId()
                    );
                    return null;
                }
            }
            log.info("Administrator not found for adminDdUuid={}", record.getAdminDdUuid());
            return null;
        } catch (Exception e) {
            log.error("Error searching admin: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Update incapacitated person to CAPABLE status (for Object Code 1003 - Lifting Administration)
     */

    private void updateIncapablePersonToCapable(IncapableDbRecord record, String onePamUuid) {
        try {
            UpdateInvolvedPartyIndividualRequestV5 individual = UpdateInvolvedPartyIndividualRequestV5.builder()
                    .dataSource(DATA_SOURCE)
                    .countryOfResidence(mapCountryCode(record.getIncCountryOfResidence()))
                    .effectiveDate(OnePamDateUtil.toOnePamDate(getCurrentDateISO()))
                    .legalCompetencyStatusType(LEGAL_COMPETENCY_CAPABLE)
                    .individualLifeCycleStatusType(INDIVIDUAL_LIFECYCLE_ACTIVE)
                    .cityOfBirth(record.getIncCityOfBirth())
                    .countryOfBirth(mapCountryCode(record.getIncCountryOfBirth()))
                    .dateOfBirth(record.getIncDateOfBirth())
                    .dateOfDeath(record.getIncDateOfDeath())
                    .gender(record.getIncGender())
                    .build();

            PatchIndividualRequest request = PatchIndividualRequest.builder()
                    .individual(individual)
                    .build();

            log.info("Data_Distributor_Incapables: Updating INC party to CAPABLE, OnePAM UUID={}", onePamUuid);
            individualRepository.updateIndividual(onePamUuid, request).get();

            log.info("Data_Distributor_Incapables: INC party set to CAPABLE successfully, OnePAM UUID={}", onePamUuid);

            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.UPDATE_INVOLVED_PARTY.endpoint,
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.method, 200);

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error updating INC party to CAPABLE: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Update INC party to CAPABLE failed: " + e.getMessage(),
                    RegkeyEnum.UPDATE_INVOLVED_PARTY.resolveEndpoint(onePamUuid),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
        }
    }

    /**
     * Close existing relationships for incapacitated person (for Object Code 1002, 1003)
     * Returns the UUID of the closed relationship if found, null otherwise.
     */
    private String closeExistingRelationships(IncapableDbRecord record, String incOnePamUuid) {
        try {
            log.info("Data_Distributor_Incapables: Fetching existing relationships to close for INC={}", incOnePamUuid);

            // Get grantees for the INC party (grantor)
            GetGranteesResponse response = identifyService.getGrantees(incOnePamUuid, record.getDdUuid(), record.getFileId());

            if (response != null && response.getInvolvedPartyInvolvedPartyRelationships() != null
                    && response.getInvolvedPartyInvolvedPartyRelationships().getAssociatedPartyRelationships() != null) {

                String closedRelUuid = null;
                for (AssociatedPartyRelationship relationship :
                        response.getInvolvedPartyInvolvedPartyRelationships().getAssociatedPartyRelationships()) {

                    String relUuid = relationship.getRelationshipIdentifier();
                    log.info("Data_Distributor_Incapables: Closing relationship UUID={}", relUuid);

                    try {
                        // Close the relationship
                        String operationalLifeCycleStatusType = "";
                        String endDate = formatDateForApi(record.getAdmResponsibilityEndDate());
                        if (endDate == null) {
/*
                            endDate = getCurrentDateISO();
*/
                            operationalLifeCycleStatusType = "EFF";

                        }else{
                             operationalLifeCycleStatusType = "IN_RTN";
                        }

                        CloseAssociatedPartyRelationshipRequest closeRequest =
                                CloseAssociatedPartyRelationshipRequest.builder()
                                        .operationalLifeCycleStatusType(operationalLifeCycleStatusType)
                                        .build();

                        relationshipService.closeAssociatedPartyRelationship(relUuid, closeRequest);


                        log.info("Data_Distributor_Incapables: Successfully closed relationship UUID={}", relUuid);
                        closedRelUuid = relUuid;

                        // Log API transaction
                        apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                                FLOW_NAME, RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.endpoint,
                                RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.method, 200);

                    } catch (Exception e) {
                        log.error("Data_Distributor_Incapables: Error closing relationship UUID={}: {}",
                                relUuid, e.getMessage(), e);
                        errorLogDAO.logApiError(
                                RecordType.INCAPABLE_RELATIONSHIP,
                                record.getIncLastName(),
                                record.getDdUuid(),
                                null,
                                "Close relationship failed: " + e.getMessage(),
                                RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.resolveEndpoint(relUuid),
                                null,
                                null,
                                null,
                                null,
                                record.getFileId()
                        );
                    }
                }
                return closedRelUuid;
            }

            log.info("Data_Distributor_Incapables: No existing relationships found to close for INC={}", incOnePamUuid);
            return null;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error fetching relationships to close: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Create a new relationship without checking for existing (for Object Code 1002 after closing)
     */
    private String createNewRelationship(IncapableDbRecord record, String incOnePamUuid, String admOnePamUuid) {
        try {
            String effectiveDate = formatDateForApi(record.getInabilityEffectiveDate());
            if (effectiveDate == null) {
                effectiveDate = OnePamDateUtil.toOnePamDate(null);
            } else {
                effectiveDate = OnePamDateUtil.toOnePamDate(effectiveDate);
            }

            // Prepare end date
            String endDate = formatDateForApi(record.getInabilityEndDate());
            if (endDate != null) {
                endDate = OnePamDateUtil.toOnePamDate(endDate);
            }

            CreateAssociatedPartyRelationshipRequest.Grantor grantor =
                    CreateAssociatedPartyRelationshipRequest.Grantor.builder()
                            .id(incOnePamUuid)
                            .build();

            // Determine operational lifecycle status based on end date
            String operationalStatus = getOperationalLifeCycleStatus(endDate);

            CreateAssociatedPartyRelationshipRequest request = CreateAssociatedPartyRelationshipRequest.builder()
                    .relationshipType(GUARDIAN_RELATIONSHIP)
                    .lifeCycleStatusType(ASSOCIATED_PARTY_RELATIONSHIP_LIFE_CYCLE_STATUS)
                    .ingEntity(ING_ENTITY)
                    .grantee(CreateAssociatedPartyRelationshipRequest.Grantee.builder()
                            .id(admOnePamUuid)
                            .build())
                    .grantors(Collections.singletonList(grantor))
                    .effectiveDate(effectiveDate)
                    .endDate(endDate)
                    .mixableIndicatorType("Y")
                    .operationalLifeCycleStatusType(operationalStatus)
                    .dataSource(DATA_SOURCE)
                    .logicalDataDomain(LOGICAL_DATA_DOMAIN)
                    .build();

            log.info("Data_Distributor_Incapables: Creating new relationship INC={} -> ADM={}, Status={}",
                    incOnePamUuid, admOnePamUuid, operationalStatus);

            CreateAssociatedPartyRelationshipResponse response =
                    relationshipService.createAssociatedPartyRelationship(request, false);
/*
            incapablesAccountingDAO.updateRelationshipOnePamSnapshot(
                    record.getDdUuid(),
                    response.getAssociatedPartyRelationship(),
                    record.getFileId()
            );*/
            String relationshipUuid = response.getAssociatedPartyRelationship().getRelationshipIdentifier();
            log.info("Data_Distributor_Incapables: New relationship created, UUID={}", relationshipUuid);

            // Log API transaction
            apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                    FLOW_NAME, RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.endpoint,
                    RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.method, 200);

            return relationshipUuid;

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error creating new relationship: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE_RELATIONSHIP,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Create new relationship failed: " + e.getMessage(),
                    RegkeyEnum.CREATE_ASSOCIATED_PARTY_RELATIONSHIP.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    /**
     * Update or create relationship with extended dates (for Object Code 1004 - New Prescription)
     */
    private String updateOrCreateRelationshipWithExtension(IncapableDbRecord record,
            String incOnePamUuid, String admOnePamUuid) {
        try {
            // First search for existing relationship
            String existingRelUuid = searchExistingRelationship(record, incOnePamUuid, admOnePamUuid);

            if (existingRelUuid != null) {
                // Update existing relationship with extended dates
                log.info("Data_Distributor_Incapables: Updating existing relationship with extended dates, UUID={}",
                        existingRelUuid);

                String newEndDate = formatDateForApi(record.getInabilityEndDate());

                UpdateAssociatedPartyRelationshipRequest updateRequest =
                        UpdateAssociatedPartyRelationshipRequest.builder()
                                .associatedPartyRelationshipLifeCycleStatusType(ASSOCIATED_PARTY_RELATIONSHIP_LIFE_CYCLE_STATUS)
                                .endDate(newEndDate) // Extend end date
                                .build();

                relationshipService.updateAssociatedPartyRelationship(existingRelUuid, updateRequest);

                log.info("Data_Distributor_Incapables: Relationship extended, UUID={}", existingRelUuid);

                // Log API transaction
                apiTransactionLogDAO.logApiSuccess(record.getDdUuid(), record.getFileId(),
                        FLOW_NAME, RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.endpoint,
                        RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.method, 200);

                return existingRelUuid;
            }
            log.info("Data_Distributor_Incapables: No existing relationship for extension, creating new");
            return createNewRelationship(record, incOnePamUuid, admOnePamUuid);

        } catch (Exception e) {
            log.error("Data_Distributor_Incapables: Error updating/creating relationship: {}", e.getMessage(), e);
            errorLogDAO.logApiError(
                    RecordType.INCAPABLE_RELATIONSHIP,
                    record.getIncLastName(),
                    record.getDdUuid(),
                    null,
                    "Update/create relationship failed: " + e.getMessage(),
                    RegkeyEnum.UPDATE_ASSOCIATED_PARTY_RELATIONSHIP.resolveEndpoint(),
                    null,
                    null,
                    null,
                    null,
                    record.getFileId()
            );
            return null;
        }
    }

    /**
     * Get current date in ISO format for API
     */
    private String getCurrentDateISO() {
        return LocalDate.now(ZoneOffset.UTC)
                .atStartOfDay(ZoneOffset.UTC)
                .format(OUTPUT_FORMAT);
    }

    /**
     * Format date from incoming format (yyyyMMdd) to API format (yyyy-MM-dd)
     */
    private String formatDateForApi(String date) {
        if (date == null || date.isEmpty()) {
            return null;
        }
        try {
            // Try yyyyMMdd format first
            if (date.length() == 8 && !date.contains("-")) {
                LocalDate parsedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyyMMdd"));
                return parsedDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
            }
            // Already in correct format or other format
            return date;
        } catch (Exception e) {
            log.warn("Data_Distributor_Incapables: Could not parse date: {}", date);
            return date;
        }
    }



    /**
     * Map country code to standard format
     */
    private String mapCountryCode(String country) {
        if (country == null || country.isEmpty()) {
            return COUNTRY_OF_RESIDENCE; // Default to BE
        }
        return country.toUpperCase();
    }

    /**
     * Check if postal address data is available
     */
    private boolean hasPostalAddress(String streetName, String postalCode) {
        return (streetName != null && !streetName.isEmpty()) ||
               (postalCode != null && !postalCode.isEmpty());
    }


    /**
     * Determine operational lifecycle status based on end date.
     * EFF - Effective if End Date is EMPTY
     * IN_RTN - In Retention if End Date is NOT EMPTY
     */
    private String getOperationalLifeCycleStatus(String endDate) {
        if (endDate == null || endDate.trim().isEmpty()) {
            return OPERATIONAL_LIFECYCLE_STATUS_TYPE_EFF;
        }
        return OPERATIONAL_LIFECYCLE_STATUS_TYPE_RETENTION;
    }

    /**
     * Get stack trace as string
     */
    private String getStackTrace(Exception e) {
        if (e == null) {
            return null;
        }
        java.io.StringWriter sw = new java.io.StringWriter();
        e.printStackTrace(new java.io.PrintWriter(sw));
        String stackTrace = sw.toString();
        return stackTrace.length() > 3900 ? stackTrace.substring(0, 3900) : stackTrace;
    }

    private String searchIncapableByInternalId(String ddUuid, String fileId) {
        try {
            SearchInvolvedPartiesResponseV1 byId = involvedPartySearchService.searchIndividualByInternalId(ddUuid, fileId);
            if (byId != null && byId.getIndividuals() != null && !byId.getIndividuals().isEmpty()) {
                int c = byId.getIndividuals().size();
                if (c == 1) {
                    String uuid = byId.getIndividuals().get(0).getInvolvedPartyIdentifier();
                    log.info("INC found by internal id (CSI_BE), UUID={}", uuid);
                    return uuid;
                } else {
                    errorLogDAO.logValidationError(
                            RecordType.INCAPABLE,
                            null,
                            "Potential INC match by internal id – multiple found: " + c,
                            ddUuid,
                            null,
                            fileId
                    );
                    return null; // manual review
                }
            }
        } catch (Exception e) {
            log.error("Error searching INC by internal id: {}", e.getMessage(), e);
        }
        return null;
    }

    private IncapableRelationship buildRelationshipWrapper(IncapableDbRecord record) {

        IncapableRelationship wrapper = new IncapableRelationship();


        wrapper.setDdUuid(record.getDdUuid());


        var inc = new com.ing.datadist.domain.incapables.IncapablePerson();
        inc.setFirstName(record.getIncFirstName());
        inc.setLastName(record.getIncLastName());
        inc.setDateOfDeath(record.getIncDateOfDeath());

        var incPostal = new com.ing.datadist.domain.incapables.PostalAddress();
        incPostal.setStreetName(record.getIncStreetName());
        incPostal.setHouseNumber(record.getIncHouseNumber());
        incPostal.setHouseNumberAddition(record.getIncHouseNumberAddition());
        incPostal.setPostalCode(record.getIncPostalCode());
        incPostal.setCityName(record.getIncCityName());
        incPostal.setCountryOfResidence(record.getIncCountryOfResidence());

        inc.setPostalAddress(incPostal);
        wrapper.setIncapablePerson(inc);


        var adm = new com.ing.datadist.domain.incapables.Administrator();
        adm.setFirstName(record.getAdmFirstName());
        adm.setLastName(record.getAdmLastName());
        adm.setResponsibilityEndDate(record.getAdmResponsibilityEndDate());

        var admPostal = new com.ing.datadist.domain.incapables.PostalAddress();
        admPostal.setStreetName(record.getAdmStreetName());
        admPostal.setHouseNumber(record.getAdmHouseNumber());
        admPostal.setHouseNumberAddition(record.getAdmHouseNumberAddition());
        admPostal.setPostalCode(record.getAdmPostalCode());
        admPostal.setCityName(record.getAdmCityName());
        admPostal.setCountryOfResidence(record.getAdmCountryOfResidence());

        adm.setPostalAddress(admPostal);
        wrapper.setAdministrator(adm);

        wrapper.setFileId(record.getFileId());

        return wrapper;
    }
    private SearchIndividualResponseV1 convert(CreateInvolvedPartyResponseV5 resp) {
        SearchIndividualResponseV1 s = new SearchIndividualResponseV1();

        if (resp == null || resp.getIndividual() == null) {
            log.warn("convert(): response or individual is null");
            s.setIndividualNames(Collections.emptyList());
            s.setPostalAddresses(Collections.emptyList());
            return s;
        }

        var ind = resp.getIndividual();

/*        // Handle names safely
        List<SearchIndividualNameResponseV1> names = Collections.emptyList();
        if (ind.getIndividualNames() != null && !ind.getIndividualNames().isEmpty()) {
            var n = ind.getIndividualNames().get(0);
            SearchIndividualNameResponseV1 name = new SearchIndividualNameResponseV1();
            name.setFirstName1(n.getFirstName1());
            name.setLastName(n.getLastName());
            names = List.of(name);
        }
        s.setIndividualNames(names);*/

        List<SearchIndividualNameResponseV1> names = new ArrayList<>();

        if (ind.getIndividualNames() != null && !ind.getIndividualNames().isEmpty()) {
            for (var n : ind.getIndividualNames()) {
                SearchIndividualNameResponseV1 nm = new SearchIndividualNameResponseV1();
                nm.setFirstName1(n.getFirstName1());
                nm.setLastName(n.getLastName());
                names.add(nm);
            }
        } else if (ind.getIndividualNames() != null) {
            var n = ind.getIndividualNames();
            SearchIndividualNameResponseV1 nm = new SearchIndividualNameResponseV1();
            nm.setFirstName1(n.get(0).getFirstName1());
            nm.setLastName(n.get(0).getLastName());
            names.add(nm);
        }

        s.setIndividualNames(names);

        // Basic fields
        s.setDateOfBirth(ind.getDateOfBirth());
        s.setCityOfBirth(ind.getCityOfBirth());
        s.setCountryOfBirth(ind.getCountryOfBirth());
        s.setDateOfDeath(ind.getDateOfDeath());

        // NEVER return null lists
        s.setPostalAddresses(Collections.emptyList());
        return s;
    }
    private SearchIndividualResponseV1 convert(CreateInvolvedPartyResponseV5 resp,
                                               PostalAddressResponse pa) {

        SearchIndividualResponseV1 s = convert(resp);

        // Build address node
        SearchOrganisationUnitAddressResponseV1 addr = new SearchOrganisationUnitAddressResponseV1();
        addr.setStreetName(pa.getStreetName());
        addr.setHouseNumber(pa.getHouseNumber());
        addr.setHouseNumberAddition(pa.getHouseNumberAddition());
        addr.setPostalCode(pa.getPostalCode());
        addr.setCityName(pa.getCityName());
        addr.setCountryCode(pa.getCountryCode());

        // ALWAYS non-null
        s.setPostalAddresses(List.of(addr));
        return s;
    }

}

package com.ing.datadist.dao;

import com.ing.datadist.api.model.AssociatedPartyRelationship;
import com.ing.datadist.api.model.InvolvedPartyMarking;
import com.ing.datadist.api.model.SearchIndividualResponseV1;
import com.ing.datadist.api.model.SearchOrganisationUnitAddressResponseV1;
import com.ing.datadist.dao.dbfields.Accounting.IncapableAccountingDbFields;
import com.ing.datadist.dao.queries.IncapablesQueries;
import com.ing.datadist.domain.incapables.Administrator;
import com.ing.datadist.domain.incapables.IncapablePerson;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import com.ing.datadist.kafka.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
@Slf4j
public class IncapablesAccountingDAO {

    private static final String TBL = "DD_INCAPABLES_ACCT_TBL";

    private final JdbcTemplate jdbcTemplate;

    public IncapablesAccountingDAO(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Individual getIndividualByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_INDIVIDUAL_BY_UUID, rs -> {
            if (rs.next()) {
                Individual individual = new Individual();
                individual.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                individual.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                individual.setGender(rs.getString(IncapableAccountingDbFields.INCP_GENDER));
                individual.setDateOfBirth(rs.getString(IncapableAccountingDbFields.INCP_DATEOFBIRTH));
                individual.setCityOfBirth(rs.getString(IncapableAccountingDbFields.INCP_CITYOFBIRTH));
                individual.setCountryOfBirth(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH));
                individual.setDateOfDeathDi(rs.getString(IncapableAccountingDbFields.INCP_DATEOFDEATH_DI));
                individual.setCountryOfResidenceDi(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
                return individual;
            }
            return null;
        }, ddUuid);
    }

    public IndividualName getIndividualNameByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_INDIVIDUAL_NAME_BY_UUID, rs -> {
            if (rs.next()) {
                IndividualName individualName = new IndividualName();
                individualName.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                individualName.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                individualName.setLastName(rs.getString(IncapableAccountingDbFields.INCP_LASTNAME));
                individualName.setFirstName1(rs.getString(IncapableAccountingDbFields.INCP_FIRSTNAME));
                return individualName;
            }
            return null;
        }, ddUuid);
    }

    public PostalAddress getIncapableAddressByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_INCAPABLE_ADDRESS_BY_UUID, rs -> {
            if (rs.next()) {
                PostalAddress address = new PostalAddress();
                address.setStreetName(rs.getString(IncapableAccountingDbFields.INCP_STREETNAME_DI));
                address.setHouseNumber(rs.getString(IncapableAccountingDbFields.INCP_HOUSENUMBER_DI));
                address.setHouseNumberAddition(rs.getString(IncapableAccountingDbFields.INCP_HOUSENUMBERADDITION_DI));
                address.setPostalCode(rs.getString(IncapableAccountingDbFields.INCP_POSTALCODE_DI));
                address.setCityName(rs.getString(IncapableAccountingDbFields.INCP_CITYNAME_DI));
                address.setCountryCode(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
                return address;
            }
            return null;
        }, ddUuid);
    }

    public IndividualName getAdministratorNameByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_ADMINISTRATOR_BY_UUID, rs -> {
            if (rs.next()) {
                IndividualName administratorName = new IndividualName();
                administratorName.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                administratorName.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                administratorName.setLastName(rs.getString(IncapableAccountingDbFields.ADM_LASTNAME_DI));
                administratorName.setFirstName1(rs.getString(IncapableAccountingDbFields.ADM_FIRSTNAME_DI));
                return administratorName;
            }
            return null;
        }, ddUuid);
    }

    public PostalAddress getAdministratorAddressByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_ADMINISTRATOR_ADDRESS_BY_UUID, rs -> {
            if (rs.next()) {
                PostalAddress address = new PostalAddress();
                address.setStreetName(rs.getString(IncapableAccountingDbFields.ADM_STREETNAME_DI));
                address.setHouseNumber(rs.getString(IncapableAccountingDbFields.ADM_HOUSENUMBER_DI));
                address.setHouseNumberAddition(rs.getString(IncapableAccountingDbFields.ADM_HOUSENUMBERADDITION_DI));
                address.setPostalCode(rs.getString(IncapableAccountingDbFields.ADM_POSTALCODE_DI));
                address.setCityName(rs.getString(IncapableAccountingDbFields.ADM_CITYNAME_DI));
                address.setCountryCode(rs.getString(IncapableAccountingDbFields.ADM_COUNTRYOFRESIDENCE_DI));
                return address;
            }
            return null;
        }, ddUuid);
    }

    public Occupation getOccupationByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_OCCUPATION_BY_UUID, rs -> {
            if (rs.next()) {
                Occupation occupation = new Occupation();
                occupation.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                occupation.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                occupation.setClassificationCode(rs.getString(IncapableAccountingDbFields.ADM_OCCUPATIONCODE_DI));
                occupation.setRank(rs.getString(IncapableAccountingDbFields.ADM_OCCUPATIONRANK));
                occupation.setEndDate(rs.getString(IncapableAccountingDbFields.ADM_RESPONSIBILITY_ENDDATE_DI));
                return occupation;
            }
            return null;
        }, ddUuid);
    }

    public com.ing.datadist.kafka.domain.Marking getMarkingByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_MARKING_BY_UUID, rs -> {
            if (rs.next()) {
                com.ing.datadist.kafka.domain.Marking marking = new com.ing.datadist.kafka.domain.Marking();
                marking.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                marking.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                marking.setObjectCodeDi(rs.getString(IncapableAccountingDbFields.OBJECT_CODE_DI));
                marking.setIncapabilityEffectiveDateDi(rs.getString(IncapableAccountingDbFields.INAB_EFFECTIVEDATE_DI));
                marking.setIncapabilityEndDateDi(rs.getString(IncapableAccountingDbFields.INAB_ENDDATE_DI));
                return marking;
            }
            return null;
        }, ddUuid);
    }

    public IncapableDomainWrapper getCompleteIncapableByUUID(String ddUuid) {
        return jdbcTemplate.query(IncapablesQueries.SELECT_COMPLETE_INCAPABLE_BY_UUID, rs -> {
            if (rs.next()) {
                IncapableDomainWrapper wrapper = new IncapableDomainWrapper();
                wrapper.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                wrapper.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));

                Individual individual = new Individual();
                individual.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
                individual.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
                individual.setGender(rs.getString(IncapableAccountingDbFields.INCP_GENDER));
                individual.setDateOfBirth(rs.getString(IncapableAccountingDbFields.INCP_DATEOFBIRTH));
                individual.setCityOfBirth(rs.getString(IncapableAccountingDbFields.INCP_CITYOFBIRTH));
                individual.setCountryOfBirth(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH));
                individual.setDateOfDeathDi(rs.getString(IncapableAccountingDbFields.INCP_DATEOFDEATH_DI));
                individual.setCountryOfResidenceDi(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
                wrapper.setIndividual(individual);

                IndividualName individualName = new IndividualName();
                individualName.setLastName(rs.getString(IncapableAccountingDbFields.INCP_LASTNAME));
                individualName.setFirstName1(rs.getString(IncapableAccountingDbFields.INCP_FIRSTNAME));
                wrapper.setIndividualName(individualName);


                return wrapper;
            }
            return null;
        }, ddUuid);
    }

    /**
     * Insert a complete incapables record into the accounting table
     * Saves both incapacitated individual and administrator data
     */
    public void insertIncapablesRecord(
            String ddUuid,
            String eventId,
            String incLastName,
            String incFirstName,
            String incGender,
            String incDateOfBirth,
            String incCityOfBirth,
            String incCountryOfBirth,
            String incStreetName,
            String incHouseNumber,
            String incHouseNumberAddition,
            String incPostalCode,
            String incCityName,
            String incCountryOfResidence,
            String incDateOfDeath,
            String admLastName,
            String admFirstName,
            String admStreetName,
            String admHouseNumber,
            String admHouseNumberAddition,
            String admPostalCode,
            String admCityName,
            String admCountryOfResidence,
            String occupationCode,
            String occupationRank,
            String objectCode,
            String inabilityEffectiveDate,
            String inabilityEndDate,
            String admResponsibilityEndDate,
            String fileId) {

        Timestamp timestamp = Timestamp.from(Instant.now());

        String sql = "INSERT INTO " + TBL + " (" +
                "DD_UUID, EVENT_ID, " +
                "INCP_LASTNAME, INCP_FIRSTNAME, INCP_GENDER, " +
                "INCP_DATEOFBIRTH, INCP_CITYOFBIRTH, INCP_COUNTRYOFBIRTH, " +
                "INCP_STREETNAME_ONEPAM, INCP_HOUSENUMBER_ONEPAM, INCP_HOUSENUMBERADDITION_ONEPAM, " +
                "INCP_POSTALCODE_ONEPAM, INCP_CITYNAME_ONEPAM, INCP_COUNTRYOFRESIDENCE_ONEPAM, " +
                "INCP_DATEOFDEATH_ONEPAM, " +
                "ADM_LASTNAME_ONEPAM, ADM_FIRSTNAME_ONEPAM, " +
                "ADM_STREETNAME_ONEPAM, ADM_HOUSENUMBER_ONEPAM, ADM_HOUSENUMBERADDITION_ONEPAM, " +
                "ADM_POSTALCODE_ONEPAM, ADM_CITYNAME_ONEPAM, ADM_COUNTRYOFRESIDENCE_ONEPAM, " +
                "ADM_OCCUPATIONCODE_ONEPAM, ADM_OCCUPATIONRANK, " +
                "OBJECT_CODE_ONEPAM, INAB_EFFECTIVEDATE_ONEPAM, INAB_ENDDATE_ONEPAM, " +
                "ADM_RESPONSIBILITY_ENDDATE_ONEPAM, " +
                "FILE_ID, CREATED_TS, UPDATED_TS) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                ddUuid, eventId,
                incLastName, incFirstName, incGender,
                incDateOfBirth, incCityOfBirth, incCountryOfBirth,
                incStreetName, incHouseNumber, incHouseNumberAddition,
                incPostalCode, incCityName, incCountryOfResidence,
                incDateOfDeath,
                admLastName, admFirstName,
                admStreetName, admHouseNumber, admHouseNumberAddition,
                admPostalCode, admCityName, admCountryOfResidence,
                occupationCode, occupationRank,
                objectCode, inabilityEffectiveDate, inabilityEndDate,
                admResponsibilityEndDate,
                fileId, timestamp, timestamp);

        log.info("Inserted incapables accounting record: DD_UUID={}, FileId={}", ddUuid, fileId);
    }

    /**
     * Update an existing incapables record in the accounting table
     * Updates both incapacitated individual and administrator data from Data Ingestion
     */
    public void updateIncapablesRecord(
            String ddUuid,
            String incLastName,
            String incFirstName,
            String incGender,
            String incDateOfBirth,
            String incCityOfBirth,
            String incCountryOfBirth,
            String incStreetName,
            String incHouseNumber,
            String incHouseNumberAddition,
            String incPostalCode,
            String incCityName,
            String incCountryOfResidence,
            String incDateOfDeath,
            String admLastName,
            String admFirstName,
            String admStreetName,
            String admHouseNumber,
            String admHouseNumberAddition,
            String admPostalCode,
            String admCityName,
            String admCountryOfResidence,
            String occupationCode,
            String occupationRank,
            String objectCode,
            String inabilityEffectiveDate,
            String inabilityEndDate,
            String admResponsibilityEndDate,
            String fileId) {

        Timestamp timestamp = Timestamp.from(Instant.now());

        String sql = "UPDATE " + TBL + " SET " +
                "INCP_LASTNAME_DI = ?, INCP_FIRSTNAME_DI = ?, INCP_GENDER = ?, " +
                "INCP_DATEOFBIRTH = ?, INCP_CITYOFBIRTH = ?, INCP_COUNTRYOFBIRTH = ?, " +
                "INCP_STREETNAME_DI = ?, INCP_HOUSENUMBER_DI = ?, INCP_HOUSENUMBERADDITION_DI = ?, " +
                "INCP_POSTALCODE_DI = ?, INCP_CITYNAME_DI = ?, INCP_COUNTRYOFRESIDENCE_DI = ?, " +
                "INCP_DATEOFDEATH_DI = ?, " +
                "ADM_LASTNAME_DI = ?, ADM_FIRSTNAME_DI = ?, " +
                "ADM_STREETNAME_DI = ?, ADM_HOUSENUMBER_DI = ?, ADM_HOUSENUMBERADDITION_DI = ?, " +
                "ADM_POSTALCODE_DI = ?, ADM_CITYNAME_DI = ?, ADM_COUNTRYOFRESIDENCE_DI = ?, " +
                "ADM_OCCUPATIONCODE_DI = ?, ADM_OCCUPATIONRANK = ?, " +
                "OBJECT_CODE_DI = ?, INAB_EFFECTIVEDATE_DI = ?, INAB_ENDDATE_DI = ?, " +
                "ADM_RESPONSIBILITY_ENDDATE_DI = ?, " +
                "FILE_ID = ?, UPDATED_TS = ? " +
                "WHERE DD_UUID = ?";

        jdbcTemplate.update(sql,
                incLastName, incFirstName, incGender,
                incDateOfBirth, incCityOfBirth, incCountryOfBirth,
                incStreetName, incHouseNumber, incHouseNumberAddition,
                incPostalCode, incCityName, incCountryOfResidence,
                incDateOfDeath,
                admLastName, admFirstName,
                admStreetName, admHouseNumber, admHouseNumberAddition,
                admPostalCode, admCityName, admCountryOfResidence,
                occupationCode, occupationRank,
                objectCode, inabilityEffectiveDate, inabilityEndDate,
                admResponsibilityEndDate,
                fileId, timestamp,
                ddUuid);

        log.info("Updated incapables accounting record: DD_UUID={}, FileId={}", ddUuid, fileId);
    }

    /**
     * Insert a new record if it doesn't exist, otherwise update it
     */
    public void insertOrUpdateIncapablesRecord(
            String ddUuid,
            String eventId,
            String incLastName,
            String incFirstName,
            String incGender,
            String incDateOfBirth,
            String incCityOfBirth,
            String incCountryOfBirth,
            String incStreetName,
            String incHouseNumber,
            String incHouseNumberAddition,
            String incPostalCode,
            String incCityName,
            String incCountryOfResidence,
            String incDateOfDeath,
            String admLastName,
            String admFirstName,
            String admStreetName,
            String admHouseNumber,
            String admHouseNumberAddition,
            String admPostalCode,
            String admCityName,
            String admCountryOfResidence,
            String occupationCode,
            String occupationRank,
            String objectCode,
            String inabilityEffectiveDate,
            String inabilityEndDate,
            String admResponsibilityEndDate,
            String fileId) {

        // Check if record exists
        String checkSql = "SELECT COUNT(*) FROM " + TBL + " WHERE DD_UUID = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, ddUuid);

        if (count != null && count > 0) {
            // Update existing record
            updateIncapablesRecord(
                    ddUuid,
                    incLastName, incFirstName, incGender,
                    incDateOfBirth, incCityOfBirth, incCountryOfBirth,
                    incStreetName, incHouseNumber, incHouseNumberAddition,
                    incPostalCode, incCityName, incCountryOfResidence,
                    incDateOfDeath,
                    admLastName, admFirstName,
                    admStreetName, admHouseNumber, admHouseNumberAddition,
                    admPostalCode, admCityName, admCountryOfResidence,
                    occupationCode, occupationRank,
                    objectCode, inabilityEffectiveDate, inabilityEndDate,
                    admResponsibilityEndDate,
                    fileId);
        } else {
            // Insert new record
            insertIncapablesRecord(
                    ddUuid, eventId,
                    incLastName, incFirstName, incGender,
                    incDateOfBirth, incCityOfBirth, incCountryOfBirth,
                    incStreetName, incHouseNumber, incHouseNumberAddition,
                    incPostalCode, incCityName, incCountryOfResidence,
                    incDateOfDeath,
                    admLastName, admFirstName,
                    admStreetName, admHouseNumber, admHouseNumberAddition,
                    admPostalCode, admCityName, admCountryOfResidence,
                    occupationCode, occupationRank,
                    objectCode, inabilityEffectiveDate, inabilityEndDate,
                    admResponsibilityEndDate,
                    fileId);
        }
    }

    public void insertWithWrapper(IncapableRelationship wrapper){

        String ddUuid = wrapper.getDdUuid();
        IncapablePerson incapablePerson = wrapper.getIncapablePerson();
        com.ing.datadist.domain.incapables.PostalAddress ipPostalAddress =incapablePerson.getPostalAddress();

        String ipGender = incapablePerson.getGender();
        String ipLastName = incapablePerson.getLastName();
        String ipFirstName = incapablePerson.getFirstName();
        String ipStreetName =ipPostalAddress.getStreetName();
        String ipHouseNumber = ipPostalAddress.getHouseNumber();
        String ipHouseNumberAdd =  ipPostalAddress.getHouseNumberAddition();
        String ipPostalCode = ipPostalAddress.getPostalCode();
        String ipCity = ipPostalAddress.getCityName();
        String ipCountryOfResidence = ipPostalAddress.getCountryOfResidence();
        String ipDateOfBirth = incapablePerson.getDateOfBirth();
        String ipDateOfDeath = incapablePerson.getDateOfDeath();
        String ipCityOfBirth = incapablePerson.getCityOfBirth();
        String ipCountryOfBirth = incapablePerson.getCountryOfBirth();

        String objectCode = wrapper.getObjectCode();
        String inabEndDate = wrapper.getInabilityEndDate();
        String inabEffectiveDate = wrapper.getInabilityEffectiveDate();

        Administrator administrator =wrapper.getAdministrator();
        com.ing.datadist.domain.incapables.PostalAddress admPostalAddress = administrator.getPostalAddress();

        String admOccupationCode = administrator.getOccupationCode();
        String admLastName = administrator.getLastName();
        String admFirstName = administrator.getFirstName();
        String admStreetName =admPostalAddress.getStreetName();
        String admHouseNumber = admPostalAddress.getHouseNumber();
        String admHouseNumberAdd =  admPostalAddress.getHouseNumberAddition();
        String admPostalCode = admPostalAddress.getPostalCode();
        String admCity = admPostalAddress.getCityName();
        String admCountryOfResidence = admPostalAddress.getCountryOfResidence();
        String admResponsibilityEndDate = administrator.getResponsibilityEndDate();
        Timestamp createTime = Timestamp.from(Instant.now());


        String sql = "INSERT INTO " + TBL + " (" +
                "DD_UUID,INCP_GENDER,INCP_LASTNAME,INCP_FIRSTNAME,INCP_STREETNAME_DI,INCP_HOUSENUMBER_DI,INCP_HOUSENUMBERADDITION_DI," +
                "INCP_POSTALCODE_DI,INCP_CITYNAME_DI,INCP_COUNTRYOFRESIDENCE_DI,INCP_DATEOFBIRTH,INCP_DATEOFDEATH_DI,INCP_CITYOFBIRTH," +
                "INCP_COUNTRYOFBIRTH,OBJECT_CODE_DI,INAB_ENDDATE_DI,INAB_EFFECTIVEDATE_DI,ADM_OCCUPATIONCODE_DI," +
                "ADM_LASTNAME_DI,ADM_FIRSTNAME_DI,ADM_STREETNAME_DI,ADM_HOUSENUMBER_DI,ADM_HOUSENUMBERADDITION_DI,ADM_POSTALCODE_DI," +
                "ADM_CITYNAME_DI,ADM_COUNTRYOFRESIDENCE_DI,ADM_RESPONSIBILITY_ENDDATE_DI," +
                "CREATED_TS" +
                ") VALUES (?, ?, ?, ?, ?, ?, ? ,?, ?, ?" +
                ", ?, ?, ?, ?, ?, ?, ? ,?, ?, ?" +
                ",?, ?, ?, ?, ?, ?, ? ,?, ?)";

        jdbcTemplate.update(sql,
                ddUuid, ipGender,ipLastName,ipFirstName,ipStreetName,ipHouseNumber,ipHouseNumberAdd,
                ipPostalCode,ipCity,ipCountryOfResidence,ipDateOfBirth,ipDateOfDeath,ipCityOfBirth,
                ipCountryOfBirth,objectCode,inabEndDate,inabEffectiveDate,admOccupationCode,
                admLastName,admFirstName,admStreetName,admHouseNumber,admHouseNumberAdd,admPostalCode,
                admCity,admCountryOfResidence,admResponsibilityEndDate,
                createTime
        );
        log.info("Inserted Incapable Record For Create, ddUuid:{}", ddUuid);
    }


    public void insertDIRecord(IncapablesDAO.IncapableDbRecord record) {

        String sql = "INSERT INTO DD_INCAPABLES_ACCT_TBL (" +
                "DD_UUID, EVENT_ID, " +
                "INCP_LASTNAME, INCP_FIRSTNAME, INCP_GENDER, " +
                "INCP_DATEOFBIRTH, INCP_CITYOFBIRTH, INCP_COUNTRYOFBIRTH, " +
                "INCP_STREETNAME_DI, INCP_HOUSENUMBER_DI, INCP_HOUSENUMBERADDITION_DI, " +
                "INCP_POSTALCODE_DI, INCP_CITYNAME_DI, INCP_COUNTRYOFRESIDENCE_DI, " +
                "INCP_DATEOFDEATH_DI, " +
                "ADM_LASTNAME_DI, ADM_FIRSTNAME_DI, " +
                "ADM_STREETNAME_DI, ADM_HOUSENUMBER_DI, ADM_HOUSENUMBERADDITION_DI, " +
                "ADM_POSTALCODE_DI, ADM_CITYNAME_DI, ADM_COUNTRYOFRESIDENCE_DI, " +
                "ADM_OCCUPATIONCODE_DI, ADM_OCCUPATIONRANK, " +
                "OBJECT_CODE_DI, INAB_EFFECTIVEDATE_DI, INAB_ENDDATE_DI, " +
                "ADM_RESPONSIBILITY_ENDDATE_DI, " +
                "FILE_ID, CREATED_TS, UPDATED_TS) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        jdbcTemplate.update(sql,
                record.getDdUuid(), "null",
                record.getIncLastName(), record.getIncFirstName(), record.getIncGender(),
                record.getIncDateOfBirth(), record.getIncCityOfBirth(), record.getIncCountryOfBirth(),
                record.getIncStreetName(), record.getIncHouseNumber(), record.getIncHouseNumberAddition(),
                record.getIncPostalCode(), record.getIncCityName(), record.getIncCountryOfResidence(),
                record.getIncDateOfDeath(),
                record.getAdmLastName(), record.getAdmFirstName(),
                record.getAdmStreetName(), record.getAdmHouseNumber(), record.getAdmHouseNumberAddition(),
                record.getAdmPostalCode(), record.getAdmCityName(), record.getAdmCountryOfResidence(),
                record.getAdmOccupationCode(),
                "1",
/*
                record.getAdmOccupationRank(),
*/
                record.getObjectCode(), record.getInabilityEffectiveDate(), record.getInabilityEndDate(),
                record.getAdmResponsibilityEndDate(),
                record.getFileId(),
                Timestamp.from(Instant.now()), Timestamp.from(Instant.now())
        );
    }
    public void updateIncapableOnePamSnapshot(String ddUuid, SearchIndividualResponseV1 inc) {

        String sql = "UPDATE " + TBL + " SET " +
                "INCP_LASTNAME = ?, INCP_FIRSTNAME = ?, " +
                "INCP_STREETNAME_ONEPAM = ?, INCP_HOUSENUMBER_ONEPAM = ?, INCP_HOUSENUMBERADDITION_ONEPAM = ?, " +
                "INCP_POSTALCODE_ONEPAM = ?, INCP_CITYNAME_ONEPAM = ?, INCP_COUNTRYOFRESIDENCE_ONEPAM = ?, " +
                "INCP_DATEOFBIRTH = ?, INCP_CITYOFBIRTH = ?, INCP_COUNTRYOFBIRTH = ?, " +
                "INCP_DATEOFDEATH_ONEPAM = ?, UPDATED_TS = SYSTIMESTAMP " +
                "WHERE DD_UUID = ?";

        var name = inc.getIndividualNames().get(0);

        var pa = inc.getPostalAddresses().get(0);

        jdbcTemplate.update(sql,
                name.getLastName(),
                name.getFirstName1(),

                pa.getStreetName(),
                pa.getHouseNumber(),
                pa.getHouseNumberAddition(),
                pa.getPostalCode(),
                pa.getCityName(),
                pa.getCountryCode(),

                inc.getDateOfBirth(),
                inc.getCityOfBirth(),
                inc.getCountryOfBirth(),
                inc.getDateOfDeath(),

                ddUuid
        );
    }

    public void updateIncapableDI(IncapablesDAO.IncapableDbRecord record) {
        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "INCP_LASTNAME=?, INCP_FIRSTNAME=?, INCP_GENDER=?, " +
                "INCP_DATEOFBIRTH=?, INCP_CITYOFBIRTH=?, INCP_COUNTRYOFBIRTH=?, " +
                "INCP_STREETNAME_DI=?, INCP_HOUSENUMBER_DI=?, INCP_HOUSENUMBERADDITION_DI=?, " +
                "INCP_POSTALCODE_DI=?, INCP_CITYNAME_DI=?, INCP_COUNTRYOFRESIDENCE_DI=?, " +
                "INCP_DATEOFDEATH_DI=?, UPDATED_TS=SYSTIMESTAMP WHERE DD_UUID=?";

        jdbcTemplate.update(sql,
                record.getIncLastName(), record.getIncFirstName(), record.getIncGender(),
                record.getIncDateOfBirth(), record.getIncCityOfBirth(), record.getIncCountryOfBirth(),
                record.getIncStreetName(), record.getIncHouseNumber(), record.getIncHouseNumberAddition(),
                record.getIncPostalCode(), record.getIncCityName(), record.getIncCountryOfResidence(),
                record.getIncDateOfDeath(),
                record.getDdUuid()
        );
    }
    public void updateAdministratorOnePamSnapshot(String ddUuid, SearchIndividualResponseV1 adm) {

        String sql = "UPDATE " + TBL + " SET " +
                "ADM_LASTNAME_ONEPAM = ?, ADM_FIRSTNAME_ONEPAM = ?, " +
                "ADM_STREETNAME_ONEPAM = ?, ADM_HOUSENUMBER_ONEPAM = ?, ADM_HOUSENUMBERADDITION_ONEPAM = ?, " +
                "ADM_POSTALCODE_ONEPAM = ?, ADM_CITYNAME_ONEPAM = ?, ADM_COUNTRYOFRESIDENCE_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP " +
                "WHERE DD_UUID = ?";

        var name = adm.getIndividualNames().get(0);
        var pa   = adm.getPostalAddresses().get(0);

        jdbcTemplate.update(sql,
                name.getLastName(),
                name.getFirstName1(),
                pa.getStreetName(),
                pa.getHouseNumber(),
                pa.getHouseNumberAddition(),
                pa.getPostalCode(),
                pa.getCityName(),
                pa.getCountryCode(),
                ddUuid
        );
    }
    public void updateMarkingOnePamSnapshot(String ddUuid,
                                            String effectiveDate,
                                            String endDate,
                                            String objectCode) {

        String sql = "UPDATE " + TBL + " SET " +
                "OBJECT_CODE_ONEPAM = ?, " +
                "INAB_EFFECTIVEDATE_ONEPAM = ?, " +
                "INAB_ENDDATE_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP " +
                "WHERE DD_UUID = ?";

        jdbcTemplate.update(sql,
                objectCode,
                effectiveDate,
                endDate,
                ddUuid
        );
    }
    public void insertInitialRow(String ddUuid, String fileId) {
        String sql = "INSERT INTO DD_INCAPABLES_ACCT_TBL (DD_UUID, EVENT_ID, FILE_ID, CREATED_BY, CREATED_TS) " +
                "VALUES (?, 'null', ?, 'DD_USER', SYSTIMESTAMP)";

        jdbcTemplate.update(sql, ddUuid, fileId);
    }
    public void updateIncapableOnePamSnapshot(
            String ddUuid,
            SearchIndividualResponseV1 resp,
            String fileId) {

        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "INCP_FIRSTNAME = ?, " +
                "INCP_LASTNAME = ?, " +
                "INCP_STREETNAME_ONEPAM = ?, " +
                "INCP_HOUSENUMBER_ONEPAM = ?, " +
                "INCP_HOUSENUMBERADDITION_ONEPAM = ?, " +
                "INCP_POSTALCODE_ONEPAM = ?, " +
                "INCP_CITYNAME_ONEPAM = ?, " +
                "INCP_COUNTRYOFRESIDENCE_ONEPAM = ?, " +
                "INCP_DATEOFDEATH_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP, FILE_ID = ? " +
                "WHERE DD_UUID = ?";

        SearchOrganisationUnitAddressResponseV1 addr =
                resp.getPostalAddresses() != null && !resp.getPostalAddresses().isEmpty()
                        ? resp.getPostalAddresses().get(0)
                        : null;

        jdbcTemplate.update(sql,
                resp.getIndividualNames().get(0).getFirstName1(),
                resp.getIndividualNames().get(0).getLastName(),
                addr != null ? addr.getStreetName() : null,
                addr != null ? addr.getHouseNumber() : null,
                addr != null ? addr.getHouseNumberAddition() : null,
                addr != null ? addr.getPostalCode() : null,
                addr != null ? addr.getCityName() : null,
                addr != null ? addr.getCountryCode() : null,
                resp.getDateOfDeath(),
                fileId,
                ddUuid
        );
    }
    public void updateAdministratorOnePamSnapshot(
            String ddUuid,
            SearchIndividualResponseV1 resp,
            String fileId) {

        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "ADM_FIRSTNAME_ONEPAM = ?, " +
                "ADM_LASTNAME_ONEPAM = ?, " +
                "ADM_STREETNAME_ONEPAM = ?, " +
                "ADM_HOUSENUMBER_ONEPAM = ?, " +
                "ADM_HOUSENUMBERADDITION_ONEPAM = ?, " +
                "ADM_POSTALCODE_ONEPAM = ?, " +
                "ADM_CITYNAME_ONEPAM = ?, " +
                "ADM_COUNTRYOFRESIDENCE_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP, FILE_ID = ? " +
                "WHERE DD_UUID = ?";

        SearchOrganisationUnitAddressResponseV1 addr =
                resp.getPostalAddresses() != null && !resp.getPostalAddresses().isEmpty()
                        ? resp.getPostalAddresses().get(0)
                        : null;

        jdbcTemplate.update(sql,
                resp.getIndividualNames().get(0).getFirstName1(),
                resp.getIndividualNames().get(0).getLastName(),
                addr != null ? addr.getStreetName() : null,
                addr != null ? addr.getHouseNumber() : null,
                addr != null ? addr.getHouseNumberAddition() : null,
                addr != null ? addr.getPostalCode() : null,
                addr != null ? addr.getCityName() : null,
                addr != null ? addr.getCountryCode() : null,
                fileId,
                ddUuid
        );
    }
    public void updateRelationshipOnePamSnapshot(
            String ddUuid,
            AssociatedPartyRelationship rel,
            String fileId) {

        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "INAB_EFFECTIVEDATE_ONEPAM = ?, " +
                "INAB_ENDDATE_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP, FILE_ID = ? " +
                "WHERE DD_UUID = ?";

        jdbcTemplate.update(sql,
                rel.getEffectiveDate(),
                rel.getEndDate(),
                fileId,
                ddUuid
        );
    }
    public void updateMarkingOnePamSnapshot(
            String ddUuid,
            InvolvedPartyMarking marking,
            String fileId) {

        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "OBJECT_CODE_ONEPAM = ?, " +
                "INAB_EFFECTIVEDATE_ONEPAM = ?, " +
                "INAB_ENDDATE_ONEPAM = ?, " +
                "UPDATED_TS = SYSTIMESTAMP, FILE_ID = ? " +
                "WHERE DD_UUID = ?";

        jdbcTemplate.update(sql,
                marking.getInvolvedPartyMarkingType(),
                marking.getEffectiveDate(),
                marking.getEndDate(),
                fileId,
                ddUuid
        );
    }
    public void updateDiFields(String ddUuid, IncapablesDAO.IncapableDbRecord record) {
        String sql = "UPDATE DD_INCAPABLES_ACCT_TBL SET " +
                "INCP_FIRSTNAME = ?, " +
                "INCP_LASTNAME = ?, " +
                "INCP_STREETNAME_DI = ?, " +
                "INCP_HOUSENUMBER_DI = ?, " +
                "INCP_HOUSENUMBERADDITION_DI = ?, " +
                "INCP_POSTALCODE_DI = ?, " +
                "INCP_CITYNAME_DI = ?, " +
                "INCP_COUNTRYOFRESIDENCE_DI = ?, " +
                "OBJECT_CODE_DI = ?, " +
                "ADM_FIRSTNAME_DI = ?, " +
                "ADM_LASTNAME_DI = ?, " +
                "ADM_STREETNAME_DI = ?, " +
                "ADM_HOUSENUMBER_DI = ?, " +
                "ADM_HOUSENUMBERADDITION_DI = ?, " +
                "ADM_POSTALCODE_DI = ?, " +
                "ADM_CITYNAME_DI = ?, " +
                "ADM_COUNTRYOFRESIDENCE_DI = ?, " +
                "INAB_EFFECTIVEDATE_DI = ?, " +
                "INAB_ENDDATE_DI = ?, " +
                "ADM_RESPONSIBILITY_ENDDATE_DI = ?, " +
                "UPDATED_TS = SYSTIMESTAMP " +
                "WHERE DD_UUID = ?";

        jdbcTemplate.update(sql,
                record.getIncFirstName(),
                record.getIncLastName(),
                record.getIncStreetName(),
                record.getIncHouseNumber(),
                record.getIncHouseNumberAddition(),
                record.getIncPostalCode(),
                record.getIncCityName(),
                record.getIncCountryOfResidence(),
                record.getObjectCode(),
                record.getAdmFirstName(),
                record.getAdmLastName(),
                record.getAdmStreetName(),
                record.getAdmHouseNumber(),
                record.getAdmHouseNumberAddition(),
                record.getAdmPostalCode(),
                record.getAdmCityName(),
                record.getAdmCountryOfResidence(),
                record.getInabilityEffectiveDate(),
                record.getInabilityEndDate(),
                record.getAdmResponsibilityEndDate(),
                ddUuid
        );
    }
    public void insertSkeletonRecord(IncapablesDAO.IncapableDbRecord r) {
        String sql = """
        INSERT INTO DD_INCAPABLES_ACCT_TBL (
            DD_UUID,
            EVENT_ID,
            FILE_ID,
            INCP_LASTNAME,
            INCP_FIRSTNAME,
            ADM_LASTNAME_DI,
            ADM_FIRSTNAME_DI,
            CREATED_TS,
            UPDATED_TS
        ) VALUES (?,?,?,?,?,?,?,SYSTIMESTAMP,SYSTIMESTAMP)
    """;

        jdbcTemplate.update(sql,
                r.getDdUuid(),
                "null",
                r.getFileId(),
                "null",
                "null",
                "null",
                "null"
        );
    }

}





NOW GIVE DB FIXES THIS IS FINAL DB LOGIC
✅ ✅ YOUR FINAL RULE CONFIRMED
When CREATE succeeds (1001 / 1004), you want:
✅ Log the OnePAM CREATE RESPONSE into _DI fields ONLY
❌ NEVER populate *_ONEPAM fields for CREATE flow
❌ NOT from CREATE individual
❌ NOT from CREATE postal address
❌ NOT from CREATE relationship
❌ NOT from CREATE marking
❌ NOT even UUID into ONEPAM columns
✅ ONEPAM fields must remain NULL in CREATE flow

✅ ONLY DI fields filled
This is exactly what you specified.
✅ ✅ FULL ACCOUNTING MATRIX (FINAL VERSION – EXACTLY AS YOU WANT)
✅ For ObjectCode 1001 & 1004
SEARCH = NULL (CREATE FLOW)
✅ Log DI fields

❌ Do NOT log _ONEPAM fields at any point

❌ Do NOT log OnePAM snapshots even after OnePam CREATE

✅ DI fields = input CSV

✅ ONEPAM fields = remain NULL
SEARCHSEARCH/IDNETIFY = NOT NULL (UPDATE FLOW)
✅ Log SEARCH/IDNETIFY(WHICHEVER NON NULL) response → into _ONEPAM fields

✅ Perform UPDATE API

✅ Log DI fields at end

✅ Log snapshots for relationship + marking (OnePAM)
✅ For ObjectCode 1002 & 1003
SEARCH = NULL
❌ STOP FLOW

❌ No DI

❌ No ONEPAM

❌ Nothing written in accounting
SEARCH/IDNETIFY = NOT NULL
✅ Update flow

✅ Write search response into ONEPAM fields(SEARCH/IDNETIFY NON NULL RESPONSE)

✅ At end write DI fields

✅ Write relationship & marking snapshots
 



