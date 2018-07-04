#  ___________________________________________________________________________
#
#  Pyomo: Python Optimization Modeling Objects
#  Copyright 2017 National Technology and Engineering Solutions of Sandia, LLC
#  Under the terms of Contract DE-NA0003525 with National Technology and 
#  Engineering Solutions of Sandia, LLC, the U.S. Government retains certain 
#  rights in this software.
#  This software is distributed under the 3-clause BSD License.
#  ___________________________________________________________________________

import sys
import logging

import pyomo.util
from pyomo.core.base import (Constraint,
                             Objective,
                             ComponentMap)
from pyomo.repn.canonical_repn import LinearCanonicalRepn
from pyomo.repn import generate_ampl_repn

from six import iteritems

def preprocess_block_objectives(block, idMap=None, loaded_modules=None):

    # Get/Create the ComponentMap for the repn
    if not hasattr(block,'_ampl_repn'):
        # print("[preprocess_block] Have to create new component map")
        block._ampl_repn = ComponentMap()
    block_ampl_repn = block._ampl_repn

    if loaded_modules is not None and 'pyomo.core.base' in loaded_modules:
        # print("[preprocess_block] loaded_modules is %s" % loaded_modules)
        ctype = loaded_modules['pyomo.core.base'].Objective
        # print("[preprocess_block] Loading ctype Objective from loaded_modules: (%s)|%s" % (ctype.__name__, ctype))
    else:
        ctype = Objective

    for objective_data in block.component_data_objects(ctype,
                                                       active=True,
                                                       descend_into=False):

        if objective_data.expr is None:
            raise ValueError("No expression has been defined for objective %s"
                             % (objective_data.name))

        try:
            # print("[preprocess_block] Generating repn with objective_data[%s] and id[%s]" %
            #       (objective_data.expr, idMap))
            ampl_repn = generate_ampl_repn(objective_data.expr,
                                           idMap=idMap)
            # print("[preprocess_block] Generated ampl_repn: %s" % len(ampl_repn._linear_vars))
        except Exception:
            err = sys.exc_info()[1]
            logging.getLogger('pyomo.core').error\
                ( "exception generating a ampl representation for objective %s: %s" \
                      % (objective_data.name, str(err)) )
            raise

        block_ampl_repn[objective_data] = ampl_repn
    # print("[preprocess_block] Finished method and made block_ampl_repn %s" % block_ampl_repn)

def preprocess_block_constraints(block, idMap=None):

    # Get/Create the ComponentMap for the repn
    if not hasattr(block,'_ampl_repn'):
        block._ampl_repn = ComponentMap()
    block_ampl_repn = block._ampl_repn

    for constraint in block.component_objects(Constraint,
                                              active=True,
                                              descend_into=False):

        preprocess_constraint(block,
                              constraint,
                              idMap=idMap,
                              block_ampl_repn=block_ampl_repn)

def preprocess_constraint(block,
                          constraint,
                          idMap=None,
                          block_ampl_repn=None):

    from pyomo.repn.beta.matrix import MatrixConstraint
    if isinstance(constraint, MatrixConstraint):
        return

    # Get/Create the ComponentMap for the repn
    if not hasattr(block,'_ampl_repn'):
        block._ampl_repn = ComponentMap()
    block_ampl_repn = block._ampl_repn

    for index, constraint_data in iteritems(constraint):

        if not constraint_data.active:
            continue

        if isinstance(constraint_data, LinearCanonicalRepn):
            continue

        if constraint_data.body is None:
            raise ValueError(
                "No expression has been defined for the body "
                "of constraint %s" % (constraint_data.name))

        try:
            ampl_repn = generate_ampl_repn(constraint_data.body,
                                           idMap=idMap)
        except Exception:
            err = sys.exc_info()[1]
            logging.getLogger('pyomo.core').error(
                "exception generating a ampl representation for "
                "constraint %s: %s"
                % (constraint_data.name, str(err)))
            raise

        block_ampl_repn[constraint_data] = ampl_repn

def preprocess_constraint_data(block,
                               constraint_data,
                               idMap=None,
                               block_ampl_repn=None):

    if isinstance(constraint_data, LinearCanonicalRepn):
        return

    # Get/Create the ComponentMap for the repn
    if not hasattr(block,'_ampl_repn'):
        block._ampl_repn = ComponentMap()
    block_ampl_repn = block._ampl_repn

    if constraint_data.body is None:
        raise ValueError(
            "No expression has been defined for the body "
            "of constraint %s" % (constraint_data.name))

    try:
        ampl_repn = generate_ampl_repn(constraint_data.body,
                                       idMap=idMap)
    except Exception:
        err = sys.exc_info()[1]
        logging.getLogger('pyomo.core').error(
            "exception generating a ampl representation for "
            "constraint %s: %s"
            % (constraint_data.name, str(err)))
        raise

    block_ampl_repn[constraint_data] = ampl_repn

@pyomo.util.pyomo_api(namespace='pyomo.repn')
def compute_ampl_repn(data, model=None):
    """
    This plugin computes the ampl representation for all objectives
    and constraints. All results are stored in a ComponentMap named
    "_ampl_repn" at the block level.

    We break out preprocessing of the objectives and constraints
    in order to avoid redundant and unnecessary work, specifically
    in contexts where a model is iteratively solved and modified.
    we don't have finer-grained resolution, but we could easily
    pass in a Constraint and an Objective if warranted.

    Required:
        model:      A concrete model instance.
    """
    idMap = {}
    for block in model.block_data_objects(active=True):
        preprocess_block_constraints(block, idMap=idMap)
        preprocess_block_objectives(block, idMap=idMap)
